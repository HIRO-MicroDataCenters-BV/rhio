#!/bin/bash

CLUSTERS=("kind-cluster1" "kind-cluster2")
POD_SUBNETS=("192.168.0.0" "192.169.0.0" "192.170.0.0")
SVC_SUBNETS=("10.96.0.0" "10.97.0.0" "10.98.0.0")
AS_NUMBERS=("65001" "65002" "65003")
DNS_DOMAINS=("cluster1.local" "cluster2.local" "cluster3.local")
DEFAULT_DNS_IPS=("10.96.0.10" "10.97.0.10" "10.98.0.10")

# NODES=("control-plane" "worker" "worker2")
NODES=("control-plane" "worker")

validate() {
  if ! command -v jq &> /dev/null; then
    echo "jq is not installed. Please install it before running this script."
    exit 1
  fi

  if ! command -v kind &> /dev/null; then
    echo "kind is not installed. Please install it before running this script."
    exit 1
  fi

}

create_clusters() {
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
EOF
}

merge_kubeconfig() {
  # Merge kubeconfig for all clusters
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
}

install_calico() {
  local cluster=$1

  echo "Installing Calico on $cluster..."    
  kubectl config use-context kind-${cluster}
  kubectl create ns calico-system
  kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.29.1/manifests/tigera-operator.yaml
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

  kubectl config use-context kind-${cluster}
  kubectl create -f -<<EOF
apiVersion: operator.tigera.io/v1
kind: Installation
metadata:
  name: default
spec:
  registry: quay.io/
  calicoNetwork:
    ipPools:
      - blockSize: 26
        cidr: ${pod_subnet}/16
        encapsulation: VXLAN
        natOutgoing: Enabled
        nodeSelector: all()
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
    kubectl config use-context kind-${cluster}
    kubectl wait --for=condition=Ready pods --all --timeout=300s -A
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
  kubectl config use-context kind-${cluster}
  kubectl get cm coredns -n kube-system -o json | \
    jq '.data.Corefile += "\n'${domain}':53 {\n	forward . '${dns_default_ip}'\n}\n"' | \
    kubectl apply -f -
  kubectl rollout restart deployment coredns -n kube-system
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
  kubectl config use-context kind-${cluster}
  kubectl create -f -<<EOF
apiVersion: projectcalico.org/v3
kind: BGPConfiguration
metadata:
  name: default
spec:
  logSeverityScreen: Info
  asNumber: ${as_number}
  serviceClusterIPs:
    - cidr: ${svc_subnet}/16
EOF
}

create_calico_node_status() {
  local cluster=$1
  local node=$2

  echo "Creating calico node status on $cluster..."    
  kubectl config use-context kind-${cluster}
  kubectl create -f -<<EOF
apiVersion: projectcalico.org/v3
kind: CalicoNodeStatus
metadata:
  name: ${node}-status
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
  local target_cluster=$1
  local node=$2
  local node_ip=$3
  local as_number=$4

  echo "Setting up BGP peer $node on target cluster $target_cluster..."    
  kubectl config use-context kind-${target_cluster}
  kubectl create  -f -<<EOF
apiVersion: projectcalico.org/v3
kind: BGPPeer
metadata:
  name: ${node}-peer
spec:
  peerIP: "${node_ip}"
  asNumber: ${as_number}
EOF
}

main() {
  validate
  create_clusters
  merge_kubeconfig
  install_calico_all
  configure_calico_all
  wait_pods_ready_all
  configure_dns_all
  configure_bgp_all
  configure_bgp_peers_all

  echo "Calico with inter-cluster networking is now set up!"
}

main