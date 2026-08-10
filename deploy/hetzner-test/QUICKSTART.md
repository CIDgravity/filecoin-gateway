# filecoin-gateway on Hetzner (FCOS + k3s)

## provision

```bash
scp deploy/hetzner-test/{config.bu,install.sh} root@RESCUE_IP:/tmp/
ssh root@RESCUE_IP

curl -sLO https://github.com/coreos/butane/releases/latest/download/butane-x86_64-unknown-linux-gnu
mv butane-x86_64-unknown-linux-gnu /usr/local/bin/butane && chmod +x /usr/local/bin/butane

export SSH_PUBKEY="ssh-ed25519 AAAA... you@host"
envsubst '$SSH_PUBKEY' < /tmp/config.bu | butane --strict > /tmp/config.ign
bash /tmp/install.sh /tmp/config.ign
reboot
```

## first boot

FCOS boots, layers packages (tmux, btop, rclone, bc), reboots once,
then installs k3s, clones repo, builds image, imports into k3s containerd.

```bash
ssh core@SERVER_IP

# check first-boot services (may need to wait for the package reboot)
journalctl -u rpm-ostree-overlay.service
journalctl -u install-k3s.service
journalctl -u setup-fgw.service
kubectl get nodes
```

## configure

```bash
# create wallet + CIDGravity account + staging config
podman run --rm -it \
  -v /var/mnt/fgw/config:/config:Z \
  -v /var/mnt/fgw/wallet:/root/.ribswallet:Z \
  localhost/fgw:local ./gwcfg -f /config/settings.env

# create wallet secret from generated keys
kubectl create secret generic wallet-keys \
  --from-file=/var/mnt/fgw/wallet/ \
  -n filecoin-gateway

# create secrets.env from template
cd /opt/filecoin-gateway
cp deploy/hetzner-test/k3s/secrets.env.example deploy/hetzner-test/k3s/secrets.env
# edit with CIDGravity token from gwcfg output
$EDITOR deploy/hetzner-test/k3s/secrets.env

# edit configmap if needed (EXTERNAL_LOCALWEB_URL, fallback providers, etc)
$EDITOR deploy/hetzner-test/k3s/configmap.yaml
```

## deploy

```bash
kubectl apply -k deploy/hetzner-test/k3s/

kubectl -n filecoin-gateway get pods -w
kubectl -n filecoin-gateway logs -f deployment/kuri
```

## DNS (required for deal-making)

kuri's builtin autocert provisions a Let's Encrypt cert on port 443,
which SPs use to pull CAR data. point a DNS A record at the server IP
with no proxy (no cloudflare orange cloud):

```
fgw-test.yourdomain.com → A → SERVER_IP
```

set `EXTERNAL_LOCALWEB_URL=https://fgw-test.yourdomain.com` in the configmap.

## test

```bash
# S3 is accessible via localhost port-forward
kubectl -n filecoin-gateway port-forward deployment/kuri 8078:8078 &

curl -s -o /dev/null -w '%{http_code}' http://localhost:8078/healthz

echo "hello" | rclone rcat fgw:test/hello.txt
rclone cat fgw:test/hello.txt

# upload test workload
./test-workload-gen.sh /var/mnt/testdata/upload
rclone copy /var/mnt/testdata/upload fgw:testbucket/ --transfers 8 --progress
```

## operations

```bash
# logs
kubectl -n filecoin-gateway logs deployment/kuri --tail=50
kubectl -n filecoin-gateway logs statefulset/yugabyte --tail=50

# dashboard (ssh tunnel)
ssh -L 9010:localhost:9010 core@SERVER_IP
# then http://localhost:9010

# restart kuri
kubectl -n filecoin-gateway rollout restart deployment/kuri

# update config
$EDITOR deploy/hetzner-test/k3s/configmap.yaml
kubectl apply -k deploy/hetzner-test/k3s/
kubectl -n filecoin-gateway rollout restart deployment/kuri

# update image (when maintainers ship a release, pull from ghcr instead)
cd /opt/filecoin-gateway && git pull
podman build . -t fgw:local
podman save localhost/fgw:local | sudo k3s ctr images import -
kubectl -n filecoin-gateway rollout restart deployment/kuri
```

## storage / redundancy

this deployment uses k3s `local-path` provisioner -- PVCs are
directories on the host NVMe with no replication or snapshots.

**storage redundancy is the operator's responsibility.** consider:
- RAID1 for OS disk, separate data disk(s) for block groups
- external block storage with snapshots (e.g. hetzner volumes)
- YugabyteDB is single-replica here; production should be RF=3

