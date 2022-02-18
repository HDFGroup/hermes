#!/bin/bash

set -x -e

if [[ "${CI}" != "true" ]]; then
    echo "This script is only meant to be run within Github actions"
    exit 1
fi

. cluster_utils.sh

# Create ssh keys for the cluster to use
echo -e 'y\n' | ssh-keygen -q -t rsa -N "" -f ~/.ssh/id_rsa

# Start a two node cluster
hermes_cluster_up

# Run the Hermes tests on the cluster (without allocating a tty)
hermes_cluster_test "-T"

# Shutdown the cluster
hermes_cluster_down
