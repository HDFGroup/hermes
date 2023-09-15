#!/bin/bash

# CD into git workspace
cd ${GITHUB_WORKSPACE}

# This script will build and install them via Spack from source
# because Hermes requires a very specific version and configuration options
# for each package.

set -x
set -e
set -o pipefail

# Change this especially when your $HOME doesn't have enough disk space. 
INSTALL_DIR="${HOME}"
SPACK_DIR=${INSTALL_DIR}/spack
SPACK_VERSION=0.18.1

echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}

# Load Spack
git clone https://github.com/spack/spack ${SPACK_DIR}
cd ${SPACK_DIR}
git checkout v${SPACK_VERSION}

# Set spack env
set +x
. ${SPACK_DIR}/share/spack/setup-env.sh
set -x

# This will allow Spack to skip building some packages that are directly
# available from the system. For example, autoconf, cmake, m4, etc.
# Modify ci/pckages.yaml to skip building compilers or build tools via Spack.
cd ${GITHUB_WORKSPACE}
cp scripts/ci/packages.yaml ${SPACK_DIR}/etc/spack/packages.yaml

# Install hermes_shm (needed for dependencies)
#
spack repo add scripts/hermes_shm
spack install hermes_shm
