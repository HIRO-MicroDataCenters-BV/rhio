#!/bin/bash
#
# This script automates the installation and setup of ArgoCD on multiple Kubernetes clusters.
# It performs the following tasks:
# 1. Installs ArgoCD on all specified clusters.
# 2. Sets up a LoadBalancer service for ArgoCD on all specified clusters.
# 3. Waits for all pods to be ready on all specified clusters.
# 4. Retrieves and displays the initial admin password for ArgoCD on all specified clusters.
#
#
CLUSTERS=("kind-cluster1" "kind-cluster2" "kind-cluster3")

export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml

main() {  
  install_argo_all
  install_balancer_all
  wait_pods_ready_all
  list_argo_credentials_all
}

install_argo_all() {
  echo "### Installing ArgoCD... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    install_argo $cluster
  done
  echo ""
}

install_argo() {
  local cluster=$1

  echo "Installing ArgoCD on $cluster"
  kubectl --context kind-${cluster} create namespace argocd
  kubectl --context kind-${cluster} apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
}

install_balancer_all() {
  echo "### Installing ArgoCD Load Balancer... ###"
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    install_balancer $cluster
  done
  echo ""
}

install_balancer() {
  local cluster=$1

  echo "Installing ArgoCD Load Balancer on $cluster"
  kubectl --context kind-${cluster} create  -f -<<EOF
apiVersion: v1
kind: Service
metadata:
  name: argocd-service-external
  namespace: argocd
spec:
  type: LoadBalancer
  externalTrafficPolicy: Local
  selector:
    app.kubernetes.io/name: argocd-server
  ports:
    - name: http
      port: 80
      protocol: TCP
      targetPort: 8080
    - name: https
      port: 443
      protocol: TCP
      targetPort: 8080
EOF
}

wait_pods_ready_all() {
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"
    echo "### Waiting till applications are up $cluster... ###"    
    kubectl --context kind-${cluster} wait --for=condition=Ready pods --all --timeout=300s -A
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
  ARGOCD_PWD=$(kubectl --context kind-${cluster} -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 --decode)
  echo "Argo CD password for cluster ${cluster} is ${ARGOCD_PWD}"
}

main