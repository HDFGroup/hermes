#!/bin/bash

set -x
set -e
set -o pipefail

INSTALL_DIR="${HOME}/${LOCAL}"
SPACK_DIR=${INSTALL_DIR}/spack
SDS_REPO_DIR=${INSTALL_DIR}/sds-repo
THALLIUM_VERSION=0.8.3
GOTCHA_VERSION=develop
CATCH2_VERSION=2.13.3

echo "Installing dependencies at ${INSTALL_DIR}"
mkdir -p ${INSTALL_DIR}
git clone https://github.com/spack/spack ${SPACK_DIR}

set +x
. ${SPACK_DIR}/share/spack/setup-env.sh
set -x

cp ci/packages.yaml ${SPACK_DIR}/etc/spack/packages.yaml
git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git ${SDS_REPO_DIR}

set +x
spack repo add ${SDS_REPO_DIR}
spack repo add ./ci/hermes

GOTCHA_SPEC=gotcha@${GOTCHA_VERSION}
spack install ${GOTCHA_SPEC}
THALLIUM_SPEC="mochi-thallium~cereal@${THALLIUM_VERSION} ^mercury~boostsys"
spack install ${THALLIUM_SPEC}
CATCH2_SPEC="catch2@${CATCH2_VERSION}"
spack install ${CATCH2_SPEC}

SPACK_STAGING_DIR=~/spack_staging
mkdir -p ${SPACK_STAGING_DIR}
spack view --verbose symlink ${SPACK_STAGING_DIR} ${THALLIUM_SPEC} ${GOTCHA_SPEC} ${CATCH2_SPEC}


set -x

ORTOOLS_VERSION=v7.7
ORTOOLS_MINOR_VERSION=7810
ORTOOLS_BASE_URL=https://github.com/google/or-tools/releases/download
ORTOOLS_TARBALL_NAME=or-tools_ubuntu-18.04_${ORTOOLS_VERSION}.${ORTOOLS_MINOR_VERSION}.tar.gz
wget ${ORTOOLS_BASE_URL}/${ORTOOLS_VERSION}/${ORTOOLS_TARBALL_NAME}
tar -xvf ${ORTOOLS_TARBALL_NAME} -C ${INSTALL_DIR} --strip-components=1

cp -LRnv ${SPACK_STAGING_DIR}/* ${INSTALL_DIR}

