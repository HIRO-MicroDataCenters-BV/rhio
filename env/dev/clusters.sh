#!/bin/bash
#
# This script manages the creation, deletion, and configuration of Kubernetes clusters using kind (Kubernetes IN Docker).
# It sets up multiple clusters with Calico for inter-cluster networking and configures BGP for routing between clusters.
#
# Usage:
#   ./clusters.sh {create|delete|cloudprovider}
#
# Commands:
#   create         - Creates the clusters, installs and configures Calico, and sets up BGP and DNS.
#   delete         - Deletes all the clusters.
#   cloudprovider  - Runs the cloud-provider-kind.
#
# Prerequisites:
#   - jq
#   - kind
#   - kubectl
#   - cloud-provider-kind
#
# Environment Variables:
#   CLUSTERS        - Array of cluster names.
#   POD_SUBNETS     - Array of pod subnets for each cluster.
#   SVC_SUBNETS     - Array of service subnets for each cluster.
#   AS_NUMBERS      - Array of BGP Autonomous System (AS) numbers for each cluster.
#   DNS_DOMAINS     - Array of DNS domains for each cluster.
#   DEFAULT_DNS_IPS - Array of default DNS IPs for each cluster.
#   NODES           - Array of node roles for each cluster.
#
CLUSTERS=("kind-cluster1" "kind-cluster2" "kind-cluster3")
POD_SUBNETS=("192.168.0.0" "192.169.0.0" "192.170.0.0")
SVC_SUBNETS=("10.96.0.0" "10.97.0.0" "10.98.0.0")
AS_NUMBERS=("65001" "65002" "65003")
DNS_DOMAINS=("cluster1.local" "cluster2.local" "cluster3.local")
DEFAULT_DNS_IPS=("10.96.0.10" "10.97.0.10" "10.98.0.10")

NODES=("control-plane" "worker" "worker2")

usage() {
    echo "Usage: $0 {create|delete|cloudprovider}"
    exit 1
}

main () {
  if [ $# -ne 1 ]; then
      usage
  fi

  case "$1" in
      create)
          create_clusters
          ;;
      delete)
          delete_clusters
          ;;
      cloudprovider)
          run_cloud_provider_kind
          ;;
      *)
          echo "Error: Invalid command '$1'"
          usage
          ;;
  esac
}

create_clusters() {
  validate
  create_clusters_all
  merge_kubeconfig
  install_calico_all
  configure_calico_all
  wait_pods_ready_all
  configure_dns_all
  wait_pods_ready_all
  configure_bgp_all
  wait_pods_ready_all
  configure_bgp_peers_all

  echo "Clusters with Calico with inter-cluster networking are now set up!"
}

validate() {
  if ! command -v jq &> /dev/null; then
    echo "jq is not installed. Please install it before running this script."
    exit 1
  fi

  if ! command -v kind &> /dev/null; then
    echo "kind is not installed. Please install it before running this script."
    exit 1
  fi

  if ! command -v kubectl &> /dev/null; then
    echo "kubectl is not installed. Please install it before running this script."
    exit 1
  fi

  if ! command -v cloud-provider-kind &> /dev/null; then
    echo "cloud-provider-kind is not installed. Please install it before running this script."
    exit 1
  fi

  mkdir -p target
}

create_clusters_all() {
  echo "### Creating clusters... ###"
  
  for i in "${!CLUSTERS[@]}"; do
      cluster="${CLUSTERS[$i]}"
      pod_subnet="${POD_SUBNETS[$i]}"
      svc_subnet="${SVC_SUBNETS[$i]}"
      dns_domain="${DNS_DOMAINS[$i]}"
      create_cluster $cluster $pod_subnet $svc_subnet $dns_domain
  done
  kind get clusters
  echo ""
}

create_cluster() {
    local cluster_name=$1
    local pod_subnet=$2
    local svc_subnet=$3
    local dns_domain=$4

    echo "Creating cluster: $cluster_name"

    kind create cluster --name $cluster_name --config - <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  podSubnet: "${pod_subnet}/16"
  serviceSubnet: "${svc_subnet}/16"
  disableDefaultCNI: true  # Disable default CNI so we can install Calico
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        ---
        apiVersion: kubeadm.k8s.io/v1beta3
        kind: ClusterConfiguration
        networking:
          dnsDomain: "${dns_domain}"
  - role: worker
  - role: worker
EOF
}

merge_kubeconfig() {

  echo "### Merging kubeconfig for all clusters... ###"
  KUBECONFIG=""
  for cluster in "${CLUSTERS[@]}"; do
      kind get kubeconfig --name ${cluster} > ./target/kubeconfig.${cluster}.yaml
      KUBECONFIG=./target/kubeconfig.${cluster}.yaml:${KUBECONFIG}:
  done

  export KUBECONFIG=${KUBECONFIG}
  kubectl config view --merge --flatten > ./target/merged-kubeconfig.yaml

  export KUBECONFIG=$(pwd)/target/merged-kubeconfig.yaml
  echo "KUBECONFIG set to: ${KUBECONFIG}"
  echo ""
}

install_calico_all() {
  echo "### Installing Calico... ###"    
  for i in "${!CLUSTERS[@]}"; do
      cluster="${CLUSTERS[$i]}"
      install_calico $cluster
  done
  echo ""    
  sleep 1
}

install_calico() {
  local cluster=$1

  echo "Installing Calico on $cluster..."    
  kubectl --context kind-${cluster} create ns calico-system
  kubectl --context kind-${cluster} create -f https://raw.githubusercontent.com/projectcalico/calico/v3.30.1/manifests/operator-crds.yaml  
  kubectl --context kind-${cluster} create -f https://raw.githubusercontent.com/projectcalico/calico/v3.30.1/manifests/tigera-operator.yaml
}

configure_calico_all() {
  echo "### Configuring Calico... ###"    

  for i in "${!CLUSTERS[@]}"; do
      cluster="${CLUSTERS[$i]}"
      pod_subnet="${POD_SUBNETS[$i]}"
      configure_calico $cluster $pod_subnet
  done
  echo ""
}

configure_calico() {
  local cluster=$1
  local pod_subnet=$2

  echo "Configuring Calico on $cluster..."    

  kubectl --context kind-${cluster} create -f -<<EOF
apiVersion: operator.tigera.io/v1
kind: Installation
metadata:
  name: default
  namespace: default
spec:
  registry: quay.io/
  calicoNetwork:
    bgp: Enabled
    ipPools:
      - blockSize: 26
        cidr: ${pod_subnet}/16
        encapsulation: None
        natOutgoing: Enabled
        nodeSelector: all()
  cni:
    type: Calico        
---
apiVersion: operator.tigera.io/v1
kind: APIServer
metadata:
  name: default
spec: {}
EOF
}

wait_pods_ready_all() {
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"
    echo "### Waiting till cluster is up $cluster... ###"    
    kubectl --context kind-${cluster} wait --for=condition=Ready pods --all --timeout=300s -A
  done
  echo ""
}

configure_dns_all() {
  echo "### Configuring DNS... ###"    
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"

    echo "Configuring DNS for cluster $cluster..."      
    for j in "${!CLUSTERS[@]}"; do
      target_cluster="${CLUSTERS[$j]}"
      domain="${DNS_DOMAINS[$j]}"
      dns_default_ip="${DEFAULT_DNS_IPS[$j]}"

      if [ "$cluster" == "$target_cluster" ]; then
        continue
      fi
      configure_dns $cluster $domain $dns_default_ip
    done
  done
  echo ""
}

configure_dns() {
  local cluster=$1
  local domain=$2
  local dns_default_ip=$3

  echo "Configuring dns domain '${domain}' resolvable via '${dns_default_ip}' on cluster $cluster..."
  kubectl --context kind-${cluster} get cm coredns -n kube-system -o json | \
    jq '.data.Corefile += "\n'${domain}':53 {\n	forward . '${dns_default_ip}'\n}\n"' | \
    kubectl --context kind-${cluster} apply -f -
  kubectl --context kind-${cluster} rollout restart deployment coredns -n kube-system
}

configure_bgp_all() {
  echo "### Configuring BGP... ###"
    
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"
    svc_subnet="${SVC_SUBNETS[$i]}"
    as_number="${AS_NUMBERS[$i]}"

    create_bgp_configuration $cluster $as_number $svc_subnet

    echo "Setting up Calico Node Statuses on cluster ${cluster}..."

    for j in "${!NODES[@]}"; do
      node_role="${NODES[$j]}"
      node="${cluster}-${node_role}"
      create_calico_node_status $cluster $node
    done  
  done
  echo ""
}

create_bgp_configuration() {
  local cluster=$1
  local as_number=$2
  local svc_subnet=$3

  echo "Setting up BGP configuration on $cluster..."    
  kubectl --context kind-${cluster} create -f -<<EOF
apiVersion: crd.projectcalico.org/v1
kind: BGPConfiguration
metadata:
  name: default
  namespace: default
spec:
  logSeverityScreen: Info
  asNumber: ${as_number}
  # nodeToNodeMeshEnabled: false
  serviceClusterIPs:
    - cidr: ${svc_subnet}/16
EOF
}

create_calico_node_status() {
  local cluster=$1
  local node=$2

  echo "Creating calico node status on $cluster..."    
  kubectl --context kind-${cluster} create -f -<<EOF
apiVersion: crd.projectcalico.org/v1
kind: CalicoNodeStatus
metadata:
  name: ${node}-status
  namespace: default
spec:
  classes:
    - Agent
    - BGP
    - Routes
  node: ${node}
  updatePeriodSeconds: 10
EOF
}

configure_bgp_peers_all() {
  echo "### Configuring BGP peers... ###"    
  for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"  
    as_number="${AS_NUMBERS[$i]}"

    for k in "${!NODES[@]}"; do
      node_role="${NODES[$k]}"
      node="${cluster}-${node_role}"
      node_ip=$(docker network inspect kind | jq -r '.[].Containers[] | select(.Name=="'"$node"'") | .IPv4Address | split("/") | .[0]')

      for j in "${!CLUSTERS[@]}"; do
        target_cluster="${CLUSTERS[$j]}"
        if [ "$cluster" == "$target_cluster" ]; then
          continue
        fi
        configure_bgp_peer $target_cluster $node $node_ip $as_number

      done
    done  
  done
  echo ""    
}

configure_bgp_peer() {
  local cluster=$1
  local node=$2
  local node_ip=$3
  local as_number=$4

  echo "Setting up BGP peer $node on target cluster $cluster..."    
  kubectl --context kind-${cluster} create  -f -<<EOF
apiVersion: projectcalico.org/v3
kind: BGPPeer
metadata:
  name: ${node}-peer
  namespace: default
spec:
  peerIP: "${node_ip}"
  asNumber: ${as_number}
EOF
}

delete_clusters() {
  validate
  delete_clusters_all
}

delete_clusters_all() {
  echo "### Deleting Clusters ..."
  for i in "${!CLUSTERS[@]}"; do
      cluster="${CLUSTERS[$i]}"
      echo "### Deleting Cluster ${cluster} ..."
      kind delete clusters ${cluster}
  done
  echo ""
}

run_cloud_provider_kind() {
  echo "### Running cloud provider kind"
  sudo cloud-provider-kind
}

main "$@"