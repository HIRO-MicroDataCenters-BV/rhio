#!/bin/bash

CLUSTERS=("kind-cluster1" "kind-cluster2")

export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml

install_argo_all() {
  echo "### Installing ArgoCD... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    install_argo $cluster
  done
}

install_argo() {
  local cluster=$1

  echo "Installing ArgoCD on $cluster"
  kubectl config use-context kind-${cluster}
  kubectl create namespace argocd
  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

}

wait_pods_ready_all() {
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"
    echo "### Waiting till applications are up $cluster... ###"    
    kubectl config use-context kind-${cluster}
    kubectl wait --for=condition=Ready pods --all --timeout=300s -A
  done
  echo ""
}

list_argo_credentials_all() {
  echo "### ArgoCD credentials... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    list_argo_credentials $cluster
  done
}

list_argo_credentials() {
  local cluster=$1  
  kubectl config use-context kind-${cluster}     
  ARGOCD_PWD=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)
  echo "Argo CD password for cluster ${cluster} is ${ARGOCD_PWD}"
}

main() {  
  install_argo_all
  wait_pods_ready_all
  list_argo_credentials_all
}

main