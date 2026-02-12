package rbstor

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/CIDgravity/filecoin-gateway/iface"
	"golang.org/x/xerrors"
)

// LoadBalancer manages distribution of writes across multiple writable groups.
// It implements weighted selection based on available space and active writers,
// with support for session affinity to keep related blocks together.
type LoadBalancer struct {
	r *rbs

	// selectionLk protects group selection logic
	// This is a lightweight lock - actual writes use per-group dataLk
	selectionLk sync.Mutex

	// Session affinity tracking
	// Maps session pointer to preferred group for locality
	sessionAffinity   map[*ribSession]iface.GroupKey
	sessionAffinityLk sync.RWMutex
}

// NewLoadBalancer creates a new load balancer for the given RBS instance.
func NewLoadBalancer(r *rbs) *LoadBalancer {
	return &LoadBalancer{
		r:               r,
		sessionAffinity: make(map[*ribSession]iface.GroupKey),
	}
}

// groupScore represents a writable group with its selection score.
type groupScore struct {
	group *Group
	score float64
}

// SelectGroup chooses the best writable group for a write operation.
// It considers:
// 1. Session affinity (if provided) - prefer same group for related blocks
// 2. Available space - prefer groups with more space
// 3. Active writers - prefer groups with fewer active writers (load balancing)
//
// Returns the selected group and a cleanup function that MUST be called after
// the write completes (successfully or not) to release the reservation.
func (lb *LoadBalancer) SelectGroup(
	ctx context.Context,
	session *ribSession,
	preferGroup iface.GroupKey,
	estimatedSize int64,
) (*Group, func(), error) {
	lb.selectionLk.Lock()
	defer lb.selectionLk.Unlock()

	cfg := configuration.GetConfig().ParallelWrite

	// Try session affinity first (if enabled and session provided)
	if session != nil && cfg.Enabled {
		if group := lb.trySessionAffinity(session, estimatedSize); group != nil {
			cleanup := func() {
				// No reservation to release for affinity hit
			}
			return group, cleanup, nil
		}
	}

	// Try preferred group (from previous write in same batch)
	if preferGroup != iface.UndefGroupKey {
		if group := lb.tryPreferredGroup(preferGroup, estimatedSize); group != nil {
			cleanup := func() {}
			return group, cleanup, nil
		}
	}

	// Fall back to weighted selection from all writable groups
	return lb.selectWeighted(ctx, session, estimatedSize, cfg)
}

// trySessionAffinity checks if the session has affinity to a group that can accept the write.
func (lb *LoadBalancer) trySessionAffinity(session *ribSession, size int64) *Group {
	start := time.Now()

	lb.sessionAffinityLk.RLock()
	affinityGroup, hasAffinity := lb.sessionAffinity[session]
	lb.sessionAffinityLk.RUnlock()

	if !hasAffinity {
		return nil
	}

	lb.r.lk.Lock()
	group, found := lb.r.writableGroups[affinityGroup]
	lb.r.lk.Unlock()

	if !found || group == nil {
		// Group no longer writable, clear affinity
		lb.clearSessionAffinity(session)
		parallelMetrics.RecordGroupSelection("affinity_miss", time.Since(start))
		return nil
	}

	// Check if group has space
	if group.AvailableSpace() < size {
		parallelMetrics.RecordGroupSelection("affinity_miss", time.Since(start))
		return nil
	}

	parallelMetrics.RecordGroupSelection("affinity_hit", time.Since(start))
	return group
}

// tryPreferredGroup attempts to use the preferred group if it has space.
func (lb *LoadBalancer) tryPreferredGroup(preferGroup iface.GroupKey, size int64) *Group {
	start := time.Now()

	lb.r.lk.Lock()
	group, found := lb.r.writableGroups[preferGroup]
	lb.r.lk.Unlock()

	if !found || group == nil {
		return nil
	}

	if group.AvailableSpace() < size {
		return nil
	}

	parallelMetrics.RecordGroupSelection("preferred", time.Since(start))
	return group
}

// selectWeighted performs weighted selection across all writable groups.
func (lb *LoadBalancer) selectWeighted(
	ctx context.Context,
	session *ribSession,
	estimatedSize int64,
	cfg configuration.ParallelWriteConfig,
) (*Group, func(), error) {
	start := time.Now()

	lb.r.lk.Lock()

	// Collect candidate groups
	var candidates []groupScore
	for _, group := range lb.r.writableGroups {
		if group.state != iface.GroupStateWritable {
			continue
		}
		// Quick check: must have space
		if group.AvailableSpace() >= estimatedSize {
			candidates = append(candidates, groupScore{group: group, score: 0})
		}
	}

	// Calculate spacing-aware scores for all candidates together
	// This encourages groups to be maximally spaced in fullness
	lb.calculateSpacingScores(candidates, estimatedSize)

	numWritable := len(lb.r.writableGroups)

	// Try to open more groups for parallelism if:
	// - Parallel writes enabled
	// - We have fewer than max groups
	if cfg.Enabled && numWritable < cfg.MaxParallelGroups {
		// Try to open existing writable group from DB first (one that isn't already open)
		allWritable, err := lb.r.db.GetAllWritableGroups(cfg.MaxParallelGroups)
		if err == nil {
			for _, info := range allWritable {
				// Skip if already open
				if _, alreadyOpen := lb.r.writableGroups[info.GroupKey]; alreadyOpen {
					continue
				}
				group, err := lb.r.openGroup(ctx, info.GroupKey, info.Blocks, info.Bytes, info.JBHead, info.State, false, true)
				if err == nil {
					if session != nil {
						lb.setSessionAffinity(session, group.id)
					}
					lb.r.lk.Unlock()
					parallelMetrics.RecordGroupSelection("parallel_open", time.Since(start))
					return group, func() {}, nil
				}
				// Failed to open this one, try next
			}
		}

		// No existing groups to open, try to create a new group
		_, group, err := lb.r.createGroup(ctx)
		if err == nil {
			if session != nil {
				lb.setSessionAffinity(session, group.id)
			}
			lb.r.lk.Unlock()
			parallelMetrics.RecordGroupSelection("parallel_created", time.Since(start))
			return group, func() {}, nil
		}
		// Failed to create, fall through to use existing candidates
	}

	// Use existing candidates if available
	if len(candidates) > 0 {
		best := lb.pickBest(candidates)
		lb.r.lk.Unlock()

		if session != nil && cfg.Enabled {
			lb.setSessionAffinity(session, best.id)
		}

		parallelMetrics.RecordGroupSelection("weighted", time.Since(start))
		return best, func() {}, nil
	}

	// No candidates available, need to open or create a group
	if !cfg.Enabled || numWritable < cfg.MaxParallelGroups {
		// Try to open existing writable group from DB
		selectedGroup, blocks, bytes, jbhead, state, err := lb.r.db.GetWritableGroup()
		if err != nil {
			lb.r.lk.Unlock()
			return nil, nil, err
		}

		if selectedGroup != iface.UndefGroupKey {
			group, err := lb.r.openGroup(ctx, selectedGroup, blocks, bytes, jbhead, state, false, true)
			if err != nil {
				lb.r.lk.Unlock()
				return nil, nil, err
			}

			if session != nil && cfg.Enabled {
				lb.setSessionAffinity(session, group.id)
			}

			lb.r.lk.Unlock()
			parallelMetrics.RecordGroupSelection("weighted", time.Since(start))
			return group, func() {}, nil
		}
	}

	// Check if we can create a new group
	if !cfg.Enabled || numWritable < cfg.MaxParallelGroups {
		_, group, err := lb.r.createGroup(ctx)
		if err != nil {
			lb.r.lk.Unlock()
			return nil, nil, err
		}

		if session != nil && cfg.Enabled {
			lb.setSessionAffinity(session, group.id)
		}

		lb.r.lk.Unlock()
		parallelMetrics.RecordGroupSelection("created", time.Since(start))
		return group, func() {}, nil
	}

	lb.r.lk.Unlock()

	// All groups are full and we can't create more
	// This shouldn't happen in normal operation
	return nil, nil, ErrNoWritableGroup
}

// spacingWeights computes write weights for groups based on their positions
// in modular [0,1) fullness space. The goal is to maintain maximally distinct
// fullness levels across all groups to avoid thundering-herd finalization.
//
// Modular space: freeRatio wraps around — when a group fills up (hits 0),
// it finalizes and gets replaced by a fresh empty one (freeRatio ≈ 1.0),
// which in circular distance is near 0. So [0,1) forms a ring.
//
// Writing to a group DECREASES its freeRatio (moves it clockwise/downward).
//
// Strategy: Find gaps between adjacent groups on the ring. Each group's
// weight is proportional to the gap BELOW it (between it and its lower
// neighbor), because writing to this group pushes it down into that gap.
// Larger gap below → more writes needed → higher weight.
//
// The minimum weight is clamped at 25% of the maximum to ensure no group
// is completely starved while converging aggressively. A group above the
// largest gap gets at most 4x the write rate of one above the smallest.
//
// Pure function on freeRatios for testability. Returns weights indexed
// same as input.
func spacingWeights(freeRatios []float64) []float64 {
	n := len(freeRatios)
	if n == 0 {
		return nil
	}
	weights := make([]float64, n)
	if n == 1 {
		weights[0] = 1.0
		return weights
	}

	// Sort by freeRatio ascending, keeping track of original indices
	type entry struct {
		origIdx int
		fr      float64
	}
	sorted := make([]entry, n)
	for i, fr := range freeRatios {
		sorted[i] = entry{i, fr}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].fr < sorted[j].fr
	})

	// Compute circular gaps.
	// gap[i] = distance from sorted[i] up to sorted[i+1] (or wrapping).
	// So gap[i] is the gap ABOVE sorted[i] / BELOW sorted[i+1].
	gaps := make([]float64, n)
	for i := 0; i < n; i++ {
		next := (i + 1) % n
		if next > 0 {
			gaps[i] = sorted[next].fr - sorted[i].fr
		} else {
			// Wrap: from highest back to lowest through 1.0→0.0
			gaps[i] = (1.0 - sorted[i].fr) + sorted[0].fr
		}
	}

	// For each group in sorted order, the gap BELOW it is gaps[(i-1+n)%n].
	// Writing to sorted[i] decreases its freeRatio, pushing it into that gap.
	// Weight ∝ gap below.
	gapBelow := make([]float64, n)
	maxGap := 0.0
	for i := 0; i < n; i++ {
		gapBelow[i] = gaps[(i-1+n)%n]
		if gapBelow[i] > maxGap {
			maxGap = gapBelow[i]
		}
	}

	if maxGap == 0 {
		// All at same position — equal weights
		for i := range weights {
			weights[i] = 1.0
		}
		return weights
	}

	// Check if all gaps are equal (co-located points with wrap anomaly)
	// When multiple groups share the same position, the wrap gap gets
	// all the distance while inter-group gaps are 0. Detect this by
	// checking if min gap is 0 and all positions are the same.
	allSame := true
	for i := 1; i < n; i++ {
		if sorted[i].fr != sorted[0].fr {
			allSame = false
			break
		}
	}
	if allSame {
		for i := range weights {
			weights[i] = 1.0
		}
		return weights
	}

	// Weight = minWeight + (1-minWeight)*(gapBelow/maxGap)
	// Range: [minWeight, 1.0]
	// Lower minWeight = more aggressive convergence but higher starvation risk
	const minWeight = 0.25
	for i := 0; i < n; i++ {
		ratio := gapBelow[i] / maxGap
		weights[sorted[i].origIdx] = minWeight + (1.0-minWeight)*ratio
	}

	return weights
}

// calculateSpacingScores computes selection weights for all candidates
// using modular spacing to maintain maximally distinct fullness levels.
func (lb *LoadBalancer) calculateSpacingScores(candidates []groupScore, estimatedSize int64) {
	n := len(candidates)
	if n == 0 {
		return
	}

	// Collect free ratios
	freeRatios := make([]float64, n)
	for i, c := range candidates {
		freeRatios[i] = float64(c.group.AvailableSpace()) / float64(maxGroupSize)
		if freeRatios[i] < 0 {
			freeRatios[i] = 0
		}
	}

	weights := spacingWeights(freeRatios)

	for i := range candidates {
		// Apply writer load factor as minor modulation
		activeWriters := float64(candidates[i].group.ActiveWriterCount())
		loadFactor := 1.0 / (1.0 + activeWriters*0.1)

		// Spacing weight dominates (80%), load factor is minor (20%)
		candidates[i].score = weights[i]*0.8 + loadFactor*0.2
	}
}

// legacy calculateScore for backward compatibility (single group scoring)
func (lb *LoadBalancer) calculateScore(group *Group, estimatedSize int64) float64 {
	available := group.AvailableSpace()
	if available < estimatedSize {
		return 0
	}
	freeRatio := float64(available) / float64(maxGroupSize)
	activeWriters := float64(group.ActiveWriterCount())
	loadFactor := 1.0 / (1.0 + activeWriters*0.1)
	return freeRatio*0.7 + loadFactor*0.3
}

// pickBest performs weighted random selection across candidates.
// Groups with higher scores have higher probability of selection,
// but no group is completely starved (unlike max-score selection).
//
// Example with 2 groups (empty vs 70% full):
//   - Before: empty group gets 100% of writes (max score wins)
//   - After: empty gets ~58%, full gets ~42% (weighted random)
func (lb *LoadBalancer) pickBest(candidates []groupScore) *Group {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0].group
	}

	// Calculate total weight
	var totalWeight float64
	for _, c := range candidates {
		totalWeight += c.score
	}

	// Weighted random selection
	// Pick a random point in [0, totalWeight) and find which candidate
	// contains that point in its cumulative weight range
	target := rand.Float64() * totalWeight
	var cumulative float64
	for _, c := range candidates {
		cumulative += c.score
		if target < cumulative {
			return c.group
		}
	}

	// Fallback (shouldn't happen due to floating point, but be safe)
	return candidates[len(candidates)-1].group
}

// setSessionAffinity records that a session prefers a specific group.
func (lb *LoadBalancer) setSessionAffinity(session *ribSession, groupKey iface.GroupKey) {
	lb.sessionAffinityLk.Lock()
	lb.sessionAffinity[session] = groupKey
	lb.sessionAffinityLk.Unlock()
}

// clearSessionAffinity removes affinity for a session.
func (lb *LoadBalancer) clearSessionAffinity(session *ribSession) {
	lb.sessionAffinityLk.Lock()
	delete(lb.sessionAffinity, session)
	lb.sessionAffinityLk.Unlock()
}

// ClearAllSessionAffinity removes all session affinities (e.g., during shutdown).
func (lb *LoadBalancer) ClearAllSessionAffinity() {
	lb.sessionAffinityLk.Lock()
	lb.sessionAffinity = make(map[*ribSession]iface.GroupKey)
	lb.sessionAffinityLk.Unlock()
}

// ErrNoWritableGroup is returned when no writable group is available.
var ErrNoWritableGroup = xerrors.New("no writable group available")

// Metrics returns current load balancer metrics.
type LoadBalancerMetrics struct {
	WritableGroupCount int
	TotalActiveWriters int32
	SessionAffinities  int
}

// Metrics returns current load balancer state for monitoring.
func (lb *LoadBalancer) Metrics() LoadBalancerMetrics {
	lb.r.lk.Lock()
	groupCount := len(lb.r.writableGroups)
	var totalWriters int32
	for _, g := range lb.r.writableGroups {
		totalWriters += g.ActiveWriterCount()
	}
	lb.r.lk.Unlock()

	lb.sessionAffinityLk.RLock()
	affinityCount := len(lb.sessionAffinity)
	lb.sessionAffinityLk.RUnlock()

	return LoadBalancerMetrics{
		WritableGroupCount: groupCount,
		TotalActiveWriters: totalWriters,
		SessionAffinities:  affinityCount,
	}
}

// parallelWritesEnabled is a cached check for whether parallel writes are enabled.
var parallelWritesEnabled atomic.Bool

func init() {
	// Will be set properly when config is loaded
	parallelWritesEnabled.Store(false)
}

// IsParallelWritesEnabled returns whether parallel writes are enabled.
func IsParallelWritesEnabled() bool {
	return parallelWritesEnabled.Load()
}

// SetParallelWritesEnabled updates the parallel writes enabled flag.
// Called when configuration is loaded.
func SetParallelWritesEnabled(enabled bool) {
	parallelWritesEnabled.Store(enabled)
	if enabled {
		log.Infow("parallel writes enabled")
	}
}
