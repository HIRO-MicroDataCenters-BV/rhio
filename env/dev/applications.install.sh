#!/bin/bash

CLUSTERS=("kind-cluster1" "kind-cluster2")

export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml

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
  kubectl config use-context kind-${cluster}
  kubectl create ns rhio
  kubectl create ns nats
  kubectl create ns minio
  kubectl create -f ./target/${cluster}.bundle.yaml
}

main() {
  generate_secrets
  kustomize_bundle_all
  install_applications_all
}

main