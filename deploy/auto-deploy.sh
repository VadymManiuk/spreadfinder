#!/usr/bin/env bash
#
# Spread Scanner auto-deploy worker.
#
# Inputs:
#   - Public GitHub remote available from the VPS
#   - Existing virtualenv and systemd service on the VPS
# Outputs:
#   - Fast-forwards the VPS checkout to origin/main
#   - Reinstalls the package, runs tests, restarts the bot service
# Assumptions:
#   - Runs as root from systemd
#   - Local tracked changes on the VPS checkout are disposable deployment state

set -Eeuo pipefail

REPO_DIR="${REPO_DIR:-/root/spreadfinder}"
BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
SERVICE_NAME="${SERVICE_NAME:-spread-scanner.service}"
PYTHON_BIN="${PYTHON_BIN:-$REPO_DIR/.venv/bin/python}"
PIP_BIN="${PIP_BIN:-$REPO_DIR/.venv/bin/pip}"

cd "$REPO_DIR"

previous_rev="$(git rev-parse HEAD)"
deployed_rev=""

rollback() {
    local exit_code=$?

    if [[ -n "$deployed_rev" ]]; then
        echo "auto_deploy_rollback from=$deployed_rev to=$previous_rev"
        git reset --hard "$previous_rev" || true
        "$PIP_BIN" install -e . || true
        systemctl restart "$SERVICE_NAME" || true
    fi

    exit "$exit_code"
}

trap rollback ERR

git fetch "$REMOTE" "$BRANCH"

remote_rev="$(git rev-parse "$REMOTE/$BRANCH")"
if [[ "$previous_rev" == "$remote_rev" ]]; then
    echo "auto_deploy_no_update rev=$previous_rev"
    exit 0
fi

echo "auto_deploy_update from=$previous_rev to=$remote_rev"

git checkout "$BRANCH"
git reset --hard "$REMOTE/$BRANCH"
deployed_rev="$remote_rev"

"$PIP_BIN" install -e .
"$PYTHON_BIN" -m pytest

systemctl restart "$SERVICE_NAME"
systemctl is-active --quiet "$SERVICE_NAME"

echo "auto_deploy_success rev=$remote_rev service=$SERVICE_NAME"
