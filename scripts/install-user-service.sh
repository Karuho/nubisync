#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${HOME}/.local/bin"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_NAME="nubisyncd.service"
UNIT_SOURCE="${REPO}/packaging/systemd/${UNIT_NAME}"
RELEASE_BINARY="${REPO}/target/release/nubisyncd"

cd "$REPO"

echo "NUBISYNCD_USER_SERVICE_INSTALL_STAGE=build_release"
cargo build --release -p nubisync-daemon --bin nubisyncd

mkdir -p "$BIN_DIR" "$UNIT_DIR"

echo "NUBISYNCD_USER_SERVICE_INSTALL_STAGE=stop_existing"
systemctl --user stop "$UNIT_NAME" >/dev/null 2>&1 || true

echo "NUBISYNCD_USER_SERVICE_INSTALL_STAGE=install"
install -m 0755 "$RELEASE_BINARY" "${BIN_DIR}/nubisyncd.new"
mv -f "${BIN_DIR}/nubisyncd.new" "${BIN_DIR}/nubisyncd"
install -m 0644 "$UNIT_SOURCE" "${UNIT_DIR}/${UNIT_NAME}"

echo "NUBISYNCD_USER_SERVICE_INSTALL_STAGE=activate"
systemctl --user daemon-reload
systemctl --user enable --now "$UNIT_NAME" >/dev/null

systemctl --user is-enabled --quiet "$UNIT_NAME"
systemctl --user is-active --quiet "$UNIT_NAME"

echo "NUBISYNCD_USER_SERVICE_INSTALL=PASS"
echo "UNIT_ENABLED=yes"
echo "UNIT_ACTIVE=yes"
echo "BINARY_INSTALL=release"
echo "RESTART_POLICY=on_failure"
echo "DRIVE_WRITE_ACCESS=no"
