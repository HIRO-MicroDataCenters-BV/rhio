# Development Environment Setup

This repository contains shell scripts to set up and manage your development environment.

## Prerequisites
- Ensure you have `kubectl`, `jq`, `kind`, `cloud-provider-kind` installed and configured.
- Ensure you have the necessary permissions to create and manage Kubernetes clusters.

## Scripts

### clusters.sh
This script is used to manage your Kubernetes clusters.

**Usage:**
```sh
./clusters.sh [options]
```

**Options:**
- `create` - Create new clusters
- `delete` - Delete existing clusters
- `cloudprovider` - Runs the cloud-provider-kind

### argocd.install.sh
This script installs Argo CD, a declarative, GitOps continuous delivery tool for Kubernetes.

**Usage:**
```sh
./argocd.install.sh
```

### applications.sh
This script manages the deployment of applications to your Kubernetes clusters.

**Usage:**
```sh
./applications.sh [options]
```

**Options:**
- `install` - Deploy applications
- `uninstall` - Delete applications
- `listsvc` - Lists all services of type LoadBalancer in all specified clusters

