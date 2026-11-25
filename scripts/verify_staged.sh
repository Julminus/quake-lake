#!/usr/bin/env bash
# Test exactly what is staged, installed from scratch.
#
# Exports the git index (not the working tree) to a temporary directory,
# installs it from the lockfile and runs every gate CI runs. A file that was
# changed but not staged, or a dependency that is only in the local .venv,
# cannot make this pass. Run it before each commit:
#
#     git add -p && bash scripts/verify_staged.sh && git commit
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

git -C "$root" checkout-index --all --force --prefix="$tmp/"
cd "$tmp"
export UV_LINK_MODE=copy

uv sync --locked --quiet
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen mypy
uv run --frozen pytest -q --cov --cov-report=term-missing:skip-covered
uv run --frozen quake-lake --help > /dev/null
# Terraform (infra/), when it is installed; CI runs these on every push either way.
if command -v terraform > /dev/null; then
  terraform -chdir=infra init -input=false -lockfile=readonly > /dev/null
  terraform -chdir=infra fmt -check -recursive
  terraform -chdir=infra validate -no-color > /dev/null
  out="$(terraform -chdir=infra test -no-color)" || { echo "$out"; exit 1; }
  echo "${out##*$'\n'}"  # the summary line
  if command -v tflint > /dev/null; then
    tflint --chdir=infra --init > /dev/null
    tflint --chdir=infra --recursive --config="$PWD/infra/.tflint.hcl"  # absolute: modules too
  fi
fi
echo "staged tree OK"
