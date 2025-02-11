#!/usr/bin/env bash

set -o errexit
set -o nounset

ROOT="${GITHUB_WORKSPACE:?Github workspace is not set.}"

VERSION_APP_PATH="${ROOT}/VERSION"
VERSION_CHART_PATH="${ROOT}/VERSION_CHART"
VERSION_DOCKER_PATH="${ROOT}/VERSION_DOCKER"
DOCKER_IMAGES_PATH="${ROOT}/DOCKER_IMAGES"

#                                         App                           Docker                            Chart
# branch, pr (e.g. "main", "mybranch"):   4.2.0.dev3-mybranch-411fa4aa  4.2.0-dev.3.mybranch.411fa4aa     4.2.0-dev.3.mybranch.411fa4aa
# tag (free text, e.g. "mytag"):          4.2.0.dev3-mytag-411fa4aa     4.2.0-dev.3.mytag.411fa4aa        4.2.0-dev.3.mytag.411fa4aa
# tag (release, e.g. "4.2.0"):            4.2.0                         4.2.0,4.2.0-411fa4aa              4.2.0
make_version() {
  GIT_SHA="$1"
  SHORT_SHA=$(echo "$GIT_SHA" | cut -c1-8)

  VERSION_BASE_HASH=$(git log --follow -1 --pretty=%H VERSION)
  VERSION_BASE=$(cat VERSION)
  GIT_COUNT=$(git rev-list --count "$VERSION_BASE_HASH"..HEAD)

  BRANCH=${GITHUB_HEAD_REF:-${GITHUB_REF##*/}}  # Branch or pr or tag
  TAG=$( [[ $GITHUB_REF == refs/tags/* ]] && echo "${GITHUB_REF##refs/tags/}" || echo "" )

  git fetch --tags --force
  git fetch --prune --unshallow || true

  LAST_RELEASE=$(get_last_release "$GIT_SHA")
  if [[ -n "$LAST_RELEASE" ]];
  then
    VERSION_BASE="$LAST_RELEASE"
    LAST_RELEASE_HASH=$(git rev-list -n 1 "$LAST_RELEASE")
    GIT_COUNT=$(git rev-list --count "$LAST_RELEASE_HASH".."$GIT_SHA")
  else
    VERSION_BASE="0.1.0"
    GIT_COUNT="0"
  fi

  echo "GIT_SHA: $GIT_SHA"
  echo "SHORT_SHA: $SHORT_SHA"
  echo "BRANCH: $BRANCH"
  echo "TAG: $TAG"
  echo "VERSION_BASE: $VERSION_BASE"
  echo "GIT_COUNT: $GIT_COUNT"

  if [[ "$TAG" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]];
  then
    VERSION_APP="$TAG"
    VERSION_CHART="$TAG"
    VERSION_DOCKER="$TAG,${TAG}-${SHORT_SHA}"
  else
    # We want to be sure that BRANCH does not contain any invalid symbols
    # and truncated to 16 symbols such that the full version has size 64 symbols maximum.
    # Otherwise this will trigger failures because we set appVersion in the helm chart to docker version.
    # appVersion from the chart (must be <64 symbols) then goes to resource label (validated using (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?)
    # (Note that full version will also get 'anomaly-detection' chart name in front of VERSION_DOCKER)
    # Docker versions are set starting from the most generic to the most specific
    # so we can take the most generic one and set to the chart values later
    BRANCH_TOKEN=$(echo "${BRANCH//[^a-zA-Z0-9-_.]/-}" | cut -c1-16 | sed -e 's/-$//')

    VERSION_APP="$VERSION_BASE+dev${GIT_COUNT}-${BRANCH_TOKEN}-${SHORT_SHA}"
    VERSION_CHART="$VERSION_BASE-dev.${GIT_COUNT}.${BRANCH_TOKEN}.${SHORT_SHA}"
    VERSION_DOCKER="$VERSION_CHART"
  fi

  echo "APP VERSION: ${VERSION_APP}"
  echo "CHART VERSION: ${VERSION_CHART}"
  echo "DOCKER VERSIONS: ${VERSION_DOCKER}"

  echo -n "${VERSION_APP}" > "${VERSION_APP_PATH}"
  echo -n "${VERSION_DOCKER}" > "${VERSION_DOCKER_PATH}"
  echo -n "${VERSION_CHART}"  > "${VERSION_CHART_PATH}"
}

get_last_release() {
  GIT_SHA="$1"

  LAST_RELEASE=$(git tag --list --merged "$GIT_SHA" --sort=-v:refname | grep -E "^[0-9]+\.[0-9]+\.[0-9]+$" | head -n 1)

  echo "$LAST_RELEASE"
}

make_docker_images_with_tags() {
  DOCKER_IMAGE_NAME="$1"
  DOCKER_IMAGE_TAGS=$(cat "${VERSION_DOCKER_PATH}")

  IFS=',' read -ra TAGS_ARRAY <<< "$DOCKER_IMAGE_TAGS"

  RESULT=""
  for TAG in "${TAGS_ARRAY[@]}"; do
    RESULT+="${DOCKER_IMAGE_NAME}:${TAG},"
  done

  RESULT=${RESULT%,}

  echo "DOCKER IMAGES WITH TAGS: ${RESULT}"

  echo -n "${RESULT}" > "${DOCKER_IMAGES_PATH}"
}

patch_versions_in_project_files() {
  VERSION_APP=$(cat "${VERSION_APP_PATH}")

  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/rhio/Cargo.toml"
  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/rhio-core/Cargo.toml"
  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/rhio-blobs/Cargo.toml"
  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/rhio-config/Cargo.toml"
  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/rhio-operator/Cargo.toml"
  sed -i "s#^version = \"[0-9a-zA-Z\.-_\+]*\"#version = \"$VERSION_APP\"#" "${ROOT}/s3-server/Cargo.toml"
}

patch_versions_in_helm_chart() {
  DOCKER_IMAGE_NAME="$1"
  CHART_NAME="$2"

  CHART_PATH="${ROOT}/charts/${CHART_NAME}"

  DOCKER_IMAGE_TAG=$(rev "${VERSION_DOCKER_PATH}" | cut -d ',' -f 1 | rev)
  VERSION_CHART=$(cat "${VERSION_CHART_PATH}")

  sed -i "s#repository: \"\"#repository: \"$DOCKER_IMAGE_NAME\"#" "${CHART_PATH}/values.yaml"
  sed -i "s#tag: \"\"#tag: \"$DOCKER_IMAGE_TAG\"#" "${CHART_PATH}/values.yaml"
  sed -i "s#version: \"[0-9a-zA-Z\.-_\+]*\"#version: \"$VERSION_CHART\"#" "${CHART_PATH}/Chart.yaml"
  sed -i "s#appVersion: \"[0-9a-zA-Z\.-_\+]*\"#appVersion: \"$VERSION_CHART\"#" "${CHART_PATH}/Chart.yaml"
}

main() {
  GIT_SHA="${1:?GIT_SHA not set}"
  DOCKER_IMAGE_NAME="${2:?Docker image name is not set}"

  make_version "$GIT_SHA"
  make_docker_images_with_tags "$DOCKER_IMAGE_NAME"
  patch_versions_in_project_files "$DOCKER_IMAGE_NAME"
  patch_versions_in_helm_chart "$DOCKER_IMAGE_NAME" "rhio"
  patch_versions_in_helm_chart "$DOCKER_IMAGE_NAME" "rhio-operator"
}

main "$@"