# Helm Chart for Stackable Operator for Rhio Service

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Rhio Service provided by Hiro.

## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)

## Install the Stackable Operator for Rhio Service

```bash
helm install rhio-operator charts/helm/rhio-operator
```

## Usage of the CRDs

The operator has example requests included in the [`/examples`](https://github.com/hiro-microdatacenters-bv/rhio/rhio-operator/tree/main/examples) directory.

## Links

<https://github.com/hiro-microdatacenters-bv/rhio>
