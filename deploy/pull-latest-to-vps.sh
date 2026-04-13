#!/usr/bin/env bash
#
# Manual VPS deploy helper for Spread Scanner.
#
# Inputs:
#   - Local git checkout with access to origin/main
#   - SSH access to the VPS checkout
# Outputs:
#   - Shows which commits are missing on the VPS
#   - Fast-forwards the VPS checkout to origin/main
#   - Reinstalls dependencies only when dependency files changed
#   - Runs pytest, restarts the bot service, prints final deployed revision
# Assumptions:
#   - The VPS checkout is on the same git history as origin/main
#   - SSH authentication is available via key or interactive password prompt

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

VPS_HOST="${VPS_HOST:-root@109.206.243.135}"
VPS_REPO_DIR="${VPS_REPO_DIR:-/root/spreadfinder}"
GIT_REMOTE="${GIT_REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
SERVICE_NAME="${SERVICE_NAME:-spread-scanner.service}"
PYTHON_BIN="${PYTHON_BIN:-$VPS_REPO_DIR/.venv/bin/python}"
PIP_BIN="${PIP_BIN:-$VPS_REPO_DIR/.venv/bin/pip}"

cd "$ROOT_DIR"

git fetch "$GIT_REMOTE" "$BRANCH" >/dev/null

local_head="$(git rev-parse HEAD)"
origin_head="$(git rev-parse "$GIT_REMOTE/$BRANCH")"
vps_head="$(
    ssh -o StrictHostKeyChecking=no "$VPS_HOST" \
        "cd '$VPS_REPO_DIR' && git rev-parse HEAD"
)"

echo "local_head=$local_head"
echo "origin_head=$origin_head"
echo "vps_head=$vps_head"

if [[ "$local_head" != "$origin_head" ]]; then
    echo "warning: local HEAD differs from $GIT_REMOTE/$BRANCH; deploy will use $GIT_REMOTE/$BRANCH"
fi

if [[ "$vps_head" == "$origin_head" ]]; then
    echo "vps_up_to_date rev=$vps_head"
    exit 0
fi

if ! git merge-base --is-ancestor "$vps_head" "$origin_head"; then
    echo "error: VPS revision $vps_head is not an ancestor of $origin_head" >&2
    exit 1
fi

echo "missing_commits_on_vps:"
git log --oneline "$vps_head..$origin_head"

ssh -o StrictHostKeyChecking=no "$VPS_HOST" \
    bash -s -- \
    "$VPS_REPO_DIR" \
    "$GIT_REMOTE" \
    "$BRANCH" \
    "$SERVICE_NAME" \
    "$PYTHON_BIN" \
    "$PIP_BIN" <<'EOF'
set -Eeuo pipefail

REPO_DIR="$1"
GIT_REMOTE="$2"
BRANCH="$3"
SERVICE_NAME="$4"
PYTHON_BIN="$5"
PIP_BIN="$6"

cd "$REPO_DIR"

previous_rev="$(git rev-parse HEAD)"

git fetch "$GIT_REMOTE" "$BRANCH" >/dev/null
target_rev="$(git rev-parse "$GIT_REMOTE/$BRANCH")"

if [[ "$previous_rev" == "$target_rev" ]]; then
    echo "remote_noop rev=$previous_rev"
    exit 0
fi

git pull --ff-only "$GIT_REMOTE" "$BRANCH"

changed_files="$(git diff --name-only "$previous_rev..$target_rev")"
if printf '%s\n' "$changed_files" | grep -Eq '^(pyproject\.toml|requirements(\..+)?\.txt|poetry\.lock|uv\.lock)$'; then
    "$PIP_BIN" install -e .
fi

"$PYTHON_BIN" -m pytest

systemctl restart "$SERVICE_NAME"
systemctl is-active --quiet "$SERVICE_NAME"

echo "remote_deploy_success rev=$target_rev service=$SERVICE_NAME"
EOF
