#!/bin/bash
# install FCOS on hetzner baremetal from rescue mode
set -euo pipefail

OS_DISK=$(ls /dev/disk/by-path/*-nvme-1 2>/dev/null | head -1)
[ -n "$OS_DISK" ] || { echo "error: no NVMe found in /dev/disk/by-path/"; exit 1; }
IGN_FILE="${1:?usage: $0 <path/to/config.ign>}"
[ -f "$IGN_FILE" ] || { echo "error: $IGN_FILE not found"; exit 1; }

REAL_DISK=$(readlink -f "$OS_DISK")
echo "target: $OS_DISK -> $REAL_DISK"
lsblk "$REAL_DISK"

read -p "this will ERASE $REAL_DISK. continue? [y/N] " -n 1 -r
echo
[[ $REPLY =~ ^[Yy]$ ]] || exit 1

if ! command -v podman &>/dev/null; then
  echo "installing podman..."
  apt-get update && apt-get install -y podman
fi

echo "installing FCOS..."
podman run --pull=always --privileged --rm --pid=host \
  --network=host \
  -v /dev:/dev -v /run/udev:/run/udev \
  -v "$(dirname "$(readlink -f "$IGN_FILE")"):/data" \
  quay.io/coreos/coreos-installer:release install \
  "$OS_DISK" \
  --ignition-file "/data/$(basename "$IGN_FILE")" \
  --stream stable

echo "done. run 'reboot' to boot into FCOS"
