#!/usr/bin/env bash
set -Eeuo pipefail

source_image="${1:?candidate image is required}"
repository="ghcr.io/aipermission/backup"
version="${GITHUB_REF_NAME#v}"
release_tags=("${GITHUB_REF_NAME}" "${version}")

tag_state() {
  local output status
  set +e
  output="$(docker manifest inspect "${repository}:$1" 2>&1)"
  status=$?
  set -e
  if [[ ${status} -eq 0 ]]; then
    printf 'exists'
    return
  fi
  if [[ "${output}" == *"manifest unknown"* || "${output}" == *"name unknown"* || "${output}" == *"not found"* ]]; then
    printf 'missing'
    return
  fi
  echo "Could not inspect ${repository}:$1: ${output}" >&2
  return 1
}

for tag in "${release_tags[@]}"; do
  if [[ "$(tag_state "${tag}")" == "exists" ]]; then
    echo "Refusing to replace immutable image tag ${repository}:${tag}." >&2
    exit 1
  fi
done

for tag in "${release_tags[@]}"; do
  docker tag "${source_image}" "${repository}:${tag}"
done
docker push "${repository}:${GITHUB_REF_NAME}"
docker push "${repository}:${version}"

if [[ "${version}" != *-* ]]; then
  docker tag "${source_image}" "${repository}:latest"
  docker push "${repository}:latest"
fi
