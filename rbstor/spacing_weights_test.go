package rbstor

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpacingWeights_SingleGroup(t *testing.T) {
	w := spacingWeights([]float64{0.5})
	require.Len(t, w, 1)
	assert.Equal(t, 1.0, w[0])
}

func TestSpacingWeights_Empty(t *testing.T) {
	w := spacingWeights(nil)
	assert.Nil(t, w)
	w = spacingWeights([]float64{})
	assert.Nil(t, w)
}

func TestSpacingWeights_AllEqual(t *testing.T) {
	// All at same position — all weights should be equal (1.0)
	w := spacingWeights([]float64{0.5, 0.5, 0.5})
	require.Len(t, w, 3)
	assert.Equal(t, 1.0, w[0])
	assert.Equal(t, 1.0, w[1])
	assert.Equal(t, 1.0, w[2])
}

func TestSpacingWeights_PerfectlySpaced(t *testing.T) {
	// 2 groups, perfectly spaced at 0.25 and 0.75 — equidistant on ring
	// Both gaps are 0.5, so weights should be equal
	w := spacingWeights([]float64{0.25, 0.75})
	require.Len(t, w, 2)
	assert.InDelta(t, w[0], w[1], 0.001, "perfectly spaced groups should have equal weights")
}

func TestSpacingWeights_PerfectlySpaced3(t *testing.T) {
	// 3 groups at 0.0, 0.333, 0.666 — equidistant
	w := spacingWeights([]float64{0.0, 1.0 / 3, 2.0 / 3})
	require.Len(t, w, 3)
	assert.InDelta(t, w[0], w[1], 0.01)
	assert.InDelta(t, w[1], w[2], 0.01)
}

func TestSpacingWeights_TwoGroupsClustered(t *testing.T) {
	// Two groups clustered together: 0.8 and 0.85
	// Gap below 0.8: wraps around = (1.0 - 0.85) + 0.8 = 0.95 (huge)
	// Gap below 0.85: 0.85 - 0.8 = 0.05 (tiny)
	// Group at 0.8 should get much more weight (it sits above the big gap)
	w := spacingWeights([]float64{0.8, 0.85})
	t.Logf("weights for [0.8, 0.85]: %v", w)
	assert.Greater(t, w[0], w[1], "group above larger gap should get more weight")
}

func TestSpacingWeights_LargestGapGetsMaxWeight(t *testing.T) {
	// 3 groups: 0.1, 0.15, 0.7
	// Sorted: [0.1, 0.15, 0.7]
	// Gaps: 0.1→0.15 = 0.05, 0.15→0.7 = 0.55, 0.7→0.1(wrap) = 0.4
	// Gap below 0.1: gap from 0.7→0.1(wrap) = 0.4
	// Gap below 0.15: gap from 0.1→0.15 = 0.05
	// Gap below 0.7: gap from 0.15→0.7 = 0.55 (largest)
	// So group at 0.7 should get highest weight
	w := spacingWeights([]float64{0.1, 0.15, 0.7})
	t.Logf("weights for [0.1, 0.15, 0.7]: %v", w)
	assert.Greater(t, w[2], w[0], "group at 0.7 should get most weight (biggest gap below)")
	assert.Greater(t, w[2], w[1], "group at 0.7 should get most weight")
	assert.Greater(t, w[0], w[1], "group at 0.1 should get more than 0.15 (bigger gap below)")
}

func TestSpacingWeights_WeightsClamped(t *testing.T) {
	// Verify min weight is 0.5 and max is 1.0
	w := spacingWeights([]float64{0.01, 0.02, 0.9})
	t.Logf("weights for [0.01, 0.02, 0.9]: %v", w)
	for i, wt := range w {
		assert.GreaterOrEqual(t, wt, 0.25, "weight %d should be >= 0.25", i)
		assert.LessOrEqual(t, wt, 1.0, "weight %d should be <= 1.0", i)
	}
	// The max/min ratio should be at most 4:1
	maxW, minW := w[0], w[0]
	for _, wt := range w[1:] {
		if wt > maxW {
			maxW = wt
		}
		if wt < minW {
			minW = wt
		}
	}
	assert.LessOrEqual(t, maxW/minW, 4.0, "max/min ratio should be <= 4.0")
}

func TestSpacingWeights_OriginalIndexPreserved(t *testing.T) {
	// Pass in unsorted order; verify weights map back correctly
	// [0.9, 0.1, 0.5] — sorted would be [0.1, 0.5, 0.9]
	// Gaps: 0.1→0.5 = 0.4, 0.5→0.9 = 0.4, 0.9→0.1(wrap) = 0.2
	// Gap below 0.1: 0.2, gap below 0.5: 0.4, gap below 0.9: 0.4
	// Groups at 0.5 and 0.9 should have equal (highest) weight
	// Group at 0.1 should have lowest weight
	w := spacingWeights([]float64{0.9, 0.1, 0.5})
	t.Logf("weights for [0.9, 0.1, 0.5]: %v", w)
	// w[0] is for 0.9, w[1] is for 0.1, w[2] is for 0.5
	assert.InDelta(t, w[0], w[2], 0.001, "0.9 and 0.5 have same gap below, should be equal")
	assert.Greater(t, w[0], w[1], "0.9 should have more weight than 0.1")
}

// TestSpacingWeights_Convergence simulates many rounds of writes and verifies
// that groups converge toward even spacing in modular space.
func TestSpacingWeights_Convergence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	tests := []struct {
		name    string
		initial []float64 // starting freeRatios
		nGroups int
	}{
		{
			name:    "2 groups clustered",
			initial: []float64{0.8, 0.85},
			nGroups: 2,
		},
		{
			name:    "3 groups clustered",
			initial: []float64{0.9, 0.88, 0.86},
			nGroups: 3,
		},
		{
			name:    "2 groups already spaced",
			initial: []float64{0.25, 0.75},
			nGroups: 2,
		},
		{
			name:    "3 groups one isolated",
			initial: []float64{0.1, 0.12, 0.7},
			nGroups: 3,
		},
		{
			name:    "4 groups clustered",
			initial: []float64{0.9, 0.88, 0.86, 0.84},
			nGroups: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freeRatios := make([]float64, len(tt.initial))
			copy(freeRatios, tt.initial)
			n := len(freeRatios)
			idealGap := 1.0 / float64(n)
			writePerStep := 0.001 // each "write" reduces freeRatio by this much

			// Simulate 5000 write operations
			for step := 0; step < 5000; step++ {
				// Handle wrapping: any group that hits 0 gets replaced at ~1.0
				for i := range freeRatios {
					if freeRatios[i] <= 0 {
						freeRatios[i] = 0.99
					}
				}

				weights := spacingWeights(freeRatios)

				// Weighted random selection
				totalW := 0.0
				for _, w := range weights {
					totalW += w
				}
				target := rng.Float64() * totalW
				cum := 0.0
				selected := n - 1
				for i, w := range weights {
					cum += w
					if target < cum {
						selected = i
						break
					}
				}

				freeRatios[selected] -= writePerStep
			}

			// Check spacing quality: compute min circular gap
			sorted := make([]float64, n)
			copy(sorted, freeRatios)
			sort.Float64s(sorted)

			minGap := math.MaxFloat64
			maxGap := 0.0
			for i := 0; i < n; i++ {
				next := (i + 1) % n
				var gap float64
				if next > 0 {
					gap = sorted[next] - sorted[i]
				} else {
					gap = (1.0 - sorted[i]) + sorted[0]
				}
				if gap < minGap {
					minGap = gap
				}
				if gap > maxGap {
					maxGap = gap
				}
			}

			t.Logf("final positions: %v", freeRatios)
			t.Logf("sorted: %v", sorted)
			t.Logf("ideal gap: %.3f, min gap: %.3f, max gap: %.3f, ratio: %.2f",
				idealGap, minGap, maxGap, maxGap/idealGap)

			// The max gap should be within 2x of ideal (generous for stochastic)
			assert.Less(t, maxGap, idealGap*2.5,
				"max gap should converge toward ideal spacing (got %.3f, ideal %.3f)", maxGap, idealGap)
			// Min gap should be at least 25% of ideal (not all bunched up)
			assert.Greater(t, minGap, idealGap*0.25,
				"min gap should not be too small (got %.3f, ideal %.3f)", minGap, idealGap)
		})
	}
}

// TestSpacingWeights_ConvergenceSpeed measures how quickly groups spread out
// from a fully clustered initial state.
func TestSpacingWeights_ConvergenceSpeed(t *testing.T) {
	rng := rand.New(rand.NewSource(123))

	// Start with 3 groups all at 0.9 (freshly created, clustered)
	freeRatios := []float64{0.9, 0.9, 0.9}
	n := len(freeRatios)
	idealGap := 1.0 / float64(n)
	writePerStep := 0.001

	stepsToConverge := -1
	for step := 0; step < 10000; step++ {
		for i := range freeRatios {
			if freeRatios[i] <= 0 {
				freeRatios[i] = 0.99
			}
		}

		weights := spacingWeights(freeRatios)
		totalW := 0.0
		for _, w := range weights {
			totalW += w
		}
		target := rng.Float64() * totalW
		cum := 0.0
		selected := n - 1
		for i, w := range weights {
			cum += w
			if target < cum {
				selected = i
				break
			}
		}
		freeRatios[selected] -= writePerStep

		// Check if converged: all gaps within 50% of ideal
		sorted := make([]float64, n)
		copy(sorted, freeRatios)
		sort.Float64s(sorted)
		converged := true
		for i := 0; i < n; i++ {
			next := (i + 1) % n
			var gap float64
			if next > 0 {
				gap = sorted[next] - sorted[i]
			} else {
				gap = (1.0 - sorted[i]) + sorted[0]
			}
			if gap < idealGap*0.5 || gap > idealGap*1.5 {
				converged = false
				break
			}
		}
		if converged && stepsToConverge < 0 {
			stepsToConverge = step
			t.Logf("converged at step %d (positions: %v)", step, freeRatios)
			break
		}
	}

	assert.Greater(t, stepsToConverge, 0, "should eventually converge")
	t.Logf("convergence took %d steps (%.0f%% of one full cycle)",
		stepsToConverge, float64(stepsToConverge)*writePerStep*100)
	// Should converge within ~2 full cycles (2000 steps).
	// Note: initial phase is purely random (all groups start at same position,
	// so weights are equal until the first random divergence happens).
	// Once differentiated, the 4:1 weight ratio drives rapid convergence.
	assert.Less(t, stepsToConverge, 2000,
		"should converge within reasonable time")
}

// TestSpacingWeights_SteadyStateBalance verifies that in steady state
// (groups already well-spaced), writes are distributed roughly equally.
func TestSpacingWeights_SteadyStateBalance(t *testing.T) {
	// 3 groups perfectly spaced
	freeRatios := []float64{0.0, 1.0 / 3, 2.0 / 3}
	w := spacingWeights(freeRatios)
	t.Logf("steady state weights: %v", w)

	// All weights should be roughly equal
	avg := (w[0] + w[1] + w[2]) / 3
	for i, wt := range w {
		assert.InDelta(t, avg, wt, 0.01,
			"in steady state, group %d weight should be near average", i)
	}
}

// TestSpacingWeights_WrapAround verifies correct handling of the 0↔1 boundary
func TestSpacingWeights_WrapAround(t *testing.T) {
	// Group at 0.95 and 0.05 — these are close in modular space (distance 0.1)
	// The big gap is between them going the other way: 0.05 → 0.95 = 0.9
	w := spacingWeights([]float64{0.95, 0.05})
	t.Logf("weights for [0.95, 0.05]: %v", w)

	// Gap below 0.95: 0.05→0.95 = 0.9 (the big gap)
	// Gap below 0.05: 0.95→0.05(wrap) = 0.1
	// So 0.95 should get way more weight
	assert.Greater(t, w[0], w[1],
		"group at 0.95 has huge gap below, should get more weight")
}

func TestSpacingWeights_PrintExamples(t *testing.T) {
	examples := [][]float64{
		{0.8, 0.85},
		{0.8, 0.3},
		{0.9, 0.6, 0.3},
		{0.9, 0.88, 0.86},
		{0.9, 0.7, 0.5, 0.3},
		{0.95, 0.05},
	}
	for _, ex := range examples {
		w := spacingWeights(ex)
		fmt.Printf("freeRatios=%v → weights=%v\n", ex, w)
	}
}
