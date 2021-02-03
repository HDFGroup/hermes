FROM gitpod/workspace-full

USER gitpod
RUN sudo apt-get update -q && \
    sudo apt-get install -yq gcc g++

RUN sudo apt-get install -y --no-install-recommends \
    autoconf \
    ca-certificates \
    curl \
    environment-modules \
    git \
    build-essential \
    python \
    nano \
    sudo \
    unzip \
    cmake \
    mpich

ENV LOCAL=$HOME
ENV PROJECT=$LOCAL/hermes
ENV INSTALL_DIR=$LOCAL/install
ENV SPACK_DIR=$LOCAL/spack
ENV SDS_DIR=$LOCAL/sds

RUN mkdir -p $INSTALL_DIR

RUN git clone https://github.com/spack/spack ${SPACK_DIR}
RUN git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git ${SDS_DIR}
RUN git clone https://github.com/HDFGroup/hermes ${PROJECT}

ENV spack=${SPACK_DIR}/bin/spack

RUN . ${SPACK_DIR}/share/spack/setup-env.sh

RUN echo $GITPOD_REPO_ROOT

RUN $spack repo add ${SDS_DIR}
RUN $spack repo add $PROJECT/ci/hermes

RUN $spack compiler find

RUN $spack compiler list

ENV THALLIUM_VERSION=0.8.3 \
    GOTCHA_VERSION=develop \
    CATCH2_VERSION=2.13.3

ENV GOTCHA_SPEC=gotcha@${GOTCHA_VERSION}
RUN $spack install ${GOTCHA_SPEC}
ENV THALLIUM_SPEC="mochi-thallium~cereal@${THALLIUM_VERSION} ^mercury~boostsys"
RUN $spack install ${THALLIUM_SPEC}
ENV CATCH2_SPEC="catch2@${CATCH2_VERSION}"
RUN $spack install ${CATCH2_SPEC}

RUN $spack view --verbose symlink ${INSTALL_DIR} ${THALLIUM_SPEC} ${GOTCHA_SPEC} ${CATCH2_SPEC}

ENV ORTOOLS_VERSION=v7.7
ENV ORTOOLS_MINOR_VERSION=7810
ENV ORTOOLS_BASE_URL=https://github.com/google/or-tools/releases/download
ENV ORTOOLS_TARBALL_NAME=or-tools_ubuntu-18.04_${ORTOOLS_VERSION}.${ORTOOLS_MINOR_VERSION}.tar.gz
RUN wget ${ORTOOLS_BASE_URL}/${ORTOOLS_VERSION}/${ORTOOLS_TARBALL_NAME}
RUN tar -xvf ${ORTOOLS_TARBALL_NAME} -C ${INSTALL_DIR} --strip-components=1
