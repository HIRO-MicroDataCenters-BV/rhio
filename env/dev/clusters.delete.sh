#!/bin/bash

# Define cluster names
CLUSTERS=("kind-cluster1" "kind-cluster2" "kind-cluster3")


for i in "${!CLUSTERS[@]}"; do
    cluster="${CLUSTERS[$i]}"
    kind delete clusters ${cluster}
done
