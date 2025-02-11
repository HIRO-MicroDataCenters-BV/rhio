#!/bin/bash

# This script manages the installation, uninstallation, and listing of services for Kubernetes clusters.
#
# It supports the following commands:
# - install: Installs applications on all specified clusters.
# - uninstall: Uninstalls applications from all specified clusters.
# - listsvc: Lists all services of type LoadBalancer in all specified clusters.
#
# Usage:
#   ./applications.sh {install|uninstall|listsvc}
#
# The script performs the following main functions:
# - Validates the presence of required tools (e.g., kubectl).
# - Generates Kubernetes secrets for each cluster.
# - Bundles Kubernetes manifests using kustomize.
# - Installs or uninstalls applications on the specified clusters.
# - Lists services of type LoadBalancer in the specified clusters.
#
# Environment Variables:
# - KUBECONFIG: Path to the merged kubeconfig file.
#

CLUSTERS=("kind-cluster1" "kind-cluster2")

export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml

usage() {
    echo "Usage: $0 {install|uninstall|listsvc}"
    exit 1
}

main() {
  if [ $# -ne 1 ]; then
      usage
  fi

  case "$1" in
      install)
          applications_install
          ;;
      uninstall)
          applications_uninstall
          ;;
      listsvc)
          services_list_all
          ;;
      *)
          echo "Error: Invalid command '$1'"
          usage
          ;;
  esac
}

validate() {

  if ! command -v kubectl &> /dev/null; then
    echo "kubectl is not installed. Please install it before running this script."
    exit 1
  fi

  mkdir -p target
}

generate_secrets() {
  echo "### Generating secrets ... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    generate_secret $cluster
  done
}

generate_secret() {
  local cluster=$1
  echo "Generatin Rhio secret for  $cluster"

  private_key=$(cat ./overlays/${cluster}/pk.txt | base64)
  public_key=$(cat ./overlays/${cluster}/pb.txt | base64)

  cat > ./overlays/${cluster}/rhio/private-key-secret.yaml <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: private-key-secret
  namespace: rhio
data:
  publicKey: ${public_key}
  secretKey: ${private_key}
EOF
}

kustomize_bundle_all() {
  echo "### Kustomize bundle ... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    kustomize_bundle $cluster
  done
}

kustomize_bundle() {
  local cluster=$1

  echo "### Kustomize bundle for cluster ${cluster}... ###"
  kustomize build ./overlays/${cluster} > ./target/${cluster}.bundle.yaml
}

install_applications_all() {
  echo "### Installing Applications ... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    install_applications $cluster
  done
}

install_applications() {
  local cluster=$1
  echo "Installing Applications on $cluster"
  kubectl --context kind-${cluster} create ns rhio
  kubectl --context kind-${cluster} create ns nats
  kubectl --context kind-${cluster} create ns minio
  kubectl --context kind-${cluster} create -f ./target/${cluster}.bundle.yaml
}

applications_install() {
  validate
  generate_secrets
  kustomize_bundle_all
  install_applications_all
}

applications_uninstall() {
  validate
  delete_applications_all
}

delete_applications_all() {
  echo "### Uninstalling Applications ... ###"

  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    delete_applications $cluster
  done
  echo ""
}

delete_applications() {
  local cluster=$1

  echo "Uninstalling Applications from $cluster"
  kubectl --context kind-${cluster} delete -f ./target/${cluster}.bundle.yaml
}

services_list_all() {
  echo "### List Services ... ###"
  validate
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    services_list $cluster
  done
  echo ""
}

services_list() {
  local cluster=$1

  echo "### List Services in cluster $cluster ..."
  kubectl --context kind-kind-cluster1 get svc -A --field-selector spec.type=LoadBalancer -o json \
    | jq -r '.items[] | select(.status.loadBalancer.ingress) | "\(.metadata.name) \(.status.loadBalancer.ingress[0].ip):\(.spec.ports[].port)"' \
    | column -t -s' '
}

main "$@"