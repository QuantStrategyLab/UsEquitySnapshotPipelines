#!/usr/bin/env bash
set -euo pipefail

workflow="${CI_WORKFLOW:-ci.yml}"
branch="${VERIFY_BRANCH:-main}"
repository="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required to verify main CI before publish." >&2
  exit 1
fi

if [ -z "${GH_TOKEN:-}" ] && [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "GH_TOKEN or GITHUB_TOKEN is required to verify main CI before publish." >&2
  exit 1
fi

export GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN}}"

read -r status conclusion <<EOF
$(gh run list \
  --repo "${repository}" \
  --workflow "${workflow}" \
  --branch "${branch}" \
  --limit 1 \
  --json status,conclusion \
  --jq '.[0] | "\(.status // "") \(.conclusion // "")"' 2>/dev/null || echo " missing")
EOF

if [ "${status}" != "completed" ] || [ "${conclusion}" != "success" ]; then
  echo "Latest ${branch} CI (${workflow}) must be completed with success before publish; got status=${status:-unknown} conclusion=${conclusion:-unknown}." >&2
  exit 1
fi

echo "Verified latest ${branch} ${workflow} run succeeded."
