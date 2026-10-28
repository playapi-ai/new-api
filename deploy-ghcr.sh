#!/usr/bin/env bash

# Usage: ./deploy-ghcr.sh [latest|version-tag]
# Configure application settings in docker-compose.yml before first deployment.
# Public GHCR image; no registry login or token is required.
# Optional default tag: NEW_API_IMAGE_TAG in .env beside this script.
set +x
set -euo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0 [tag] (default: NEW_API_IMAGE_TAG from environment/.env, then latest)"
  echo "Image: ghcr.io/playapi-ai/new-api"
  echo "Public image: no registry login or token is required."
  echo "Application settings: docker-compose.yml; see .env.example for deployment options."
  exit 0
fi

if [[ $# -gt 1 ]]; then
  echo "Only one image tag is accepted." >&2
  exit 1
fi

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$project_root"

# Read only deployment values; never source .env or execute its contents.
if [[ -f .env ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    if [[ ! "$line" =~ ^[[:space:]]*(export[[:space:]]+)?(NEW_API_IMAGE_TAG)[[:space:]]*=(.*)$ ]]; then
      continue
    fi
    name="${BASH_REMATCH[2]}"
    value="${BASH_REMATCH[3]}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "$value" == \"* || "$value" == \'* ]]; then
      quote="${value:0:1}"
      value="${value:1}"
      if [[ "$value" != *"$quote"* ]]; then
        echo "Invalid quoted deployment value in .env: $name" >&2
        exit 1
      fi
      suffix="${value#*"$quote"}"
      suffix="${suffix#"${suffix%%[![:space:]]*}"}"
      if [[ -n "$suffix" && "$suffix" != \#* ]]; then
        echo "Unexpected text after deployment value in .env: $name" >&2
        exit 1
      fi
      value="${value%%"$quote"*}"
    else
      value="${value%%[[:space:]]#*}"
      value="${value%"${value##*[![:space:]]}"}"
    fi
    file_tag="$value"
  done < .env
fi

# Explicit arguments override the environment, which overrides .env.
tag="${1-${NEW_API_IMAGE_TAG-${file_tag:-latest}}}"
unset file_tag line value

if [[ ! "$tag" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]]; then
  echo "Invalid Docker image tag." >&2
  exit 1
fi
command -v docker >/dev/null || { echo "Please install Docker and Compose v2." >&2; exit 1; }
docker compose version >/dev/null
docker info >/dev/null

export NEW_API_IMAGE_TAG="$tag"
compose=(docker compose -f "$project_root/docker-compose.yml" -f "$project_root/docker-compose.ghcr.yml")
"${compose[@]}" config --quiet

echo "Pulling ghcr.io/playapi-ai/new-api:$tag ..."
# Refresh the application image; download dependency images only if missing.
# Finish all pulls before changing running containers.
"${compose[@]}" pull new-api
"${compose[@]}" pull --policy missing
if ! "${compose[@]}" up -d --no-build --pull never --wait --wait-timeout 180; then
  "${compose[@]}" ps >&2 || true
  echo "Deployment did not become ready within the startup check. Inspect Compose logs before retrying." >&2
  exit 1
fi
"${compose[@]}" ps
echo "Deployed ghcr.io/playapi-ai/new-api:$tag. The default HTTP port is 3000."
