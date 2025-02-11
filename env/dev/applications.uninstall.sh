#!/bin/bash

CLUSTERS=("kind-cluster1" "kind-cluster2" "kind-cluster3")

export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml

delete_applications_all() {
  echo "### Uninstalling Applications ... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    delete_applications $cluster
  done
}

delete_applications() {
  local cluster=$1
  echo "Uninstalling Applications from $cluster"
  kubectl config use-context kind-${cluster}
  kubectl delete -f ./target/${cluster}.bundle.yaml
}

delete_applications_all
