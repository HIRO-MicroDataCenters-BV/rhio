#!/usr/bin/env bash

set -o errexit
set -o nounset

ROOT="${GITHUB_WORKSPACE}"
CHART_NAME="server"

VERSION_APP_PATH="./VERSION_APP"
VERSION_CHART_PATH="./VERSION_CHART"
VERSION_DOCKER_PATH="./VERSION_DOCKER"
DOCKER_IMAGES_PATH="./DOCKER_IMAGES"

#                                         App                           Docker                            Chart
# branch, pr (e.g. "main", "mybranch"):   4.2.0.dev3-mybranch-411fa4aa  4.2.0-dev.3.mybranch.411fa4aa     4.2.0-dev.3.mybranch.411fa4aa
# tag (free text, e.g. "mytag"):          4.2.0.dev3-mytag-411fa4aa     4.2.0-dev.3.mytag.411fa4aa        4.2.0-dev.3.mytag.411fa4aa
# tag (release, e.g. "4.2.0"):            4.2.0                         4.2.0,4.2.0-411fa4aa              4.2.0
make_version() {
  GIT_SHA=$(git log -1 --pretty=%H)
  SHORT_SHA=$(echo "$GIT_SHA" | cut -c1-8)

  VERSION_BASE_HASH=$(git log --follow -1 --pretty=%H VERSION)
  VERSION_BASE=$(cat VERSION)
  GIT_COUNT=$(git rev-list --count "$VERSION_BASE_HASH"..HEAD)

  BRANCH=${GITHUB_HEAD_REF:-${GITHUB_REF##*/}}  # Branch or pr or tag
  TAG=$( [[ $GITHUB_REF == refs/tags/* ]] && echo "${GITHUB_REF##refs/tags/}" || echo "" )

  echo "GIT_SHA: $GIT_SHA"
  echo "SHORT_SHA: $SHORT_SHA"
  echo "BRANCH: $BRANCH"
  echo "TAG: $TAG"
  echo "VERSION_BASE_HASH: $VERSION_BASE_HASH"
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

patch_helm_chart() {
  DOCKER_IMAGE_NAME="$1"

  CHART_PATH="./server/charts/${CHART_NAME}"

  DOCKER_IMAGE_TAG=$(rev "${VERSION_DOCKER_PATH}" | cut -d ',' -f 1 | rev)
  VERSION_CHART=$(cat "${VERSION_CHART_PATH}")

  sed -i "s#repository: \"\"#repository: \"$DOCKER_IMAGE_NAME\"#" "${CHART_PATH}/values.yaml"
  sed -i "s#tag: \"\"#tag: \"$DOCKER_IMAGE_TAG\"#" "${CHART_PATH}/values.yaml"
  sed -i "s#version: \"\"#version: \"$VERSION_CHART\"#" "${CHART_PATH}/Chart.yaml"
  sed -i "s#appVersion: \"\"#appVersion: \"$VERSION_CHART\"#" "${CHART_PATH}/Chart.yaml"
}

main() {
  DOCKER_IMAGE_NAME="$1"

  cd "$ROOT"

  make_version
  make_docker_images_with_tags "$DOCKER_IMAGE_NAME"
  patch_helm_chart "$DOCKER_IMAGE_NAME"
}

main "$@"