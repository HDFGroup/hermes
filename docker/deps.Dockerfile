# Install ubuntu 22.04
FROM ubuntu:22.04
LABEL maintainer="llogan@hawk.iit.edu"
LABEL version="0.0"
LABEL description="Hermes Docker image with CI"

# Disable Prompt During Packages Installation
ARG DEBIAN_FRONTEND=noninteractive

# Update ubuntu
RUN apt update && apt install

# Install some basic packages
RUN apt install -y \
    openssh-server \
    sudo \
    git \
    gcc g++ gfortran make binutils gpg \
    tar zip xz-utils 7zip bzip2 \
    perl m4 libncurses5-dev libxml2-dev diffutils \
    pkg-config cmake pkg-config \
    python3 python3-pip doxygen \
    lcov zlib1g-dev hdf5-tools \
    lmod

ENV INSTALL_DIR="${HOME}"
ENV SPACK_DIR="${INSTALL_DIR}/spack"
ENV SPACK_VERSION="v0.20.2"
ENV HERMES_DEPS_DIR="${HOME}/hermes_deps"
ENV HERMES_DIR="${HOME}/hermes"

# Install Spack
RUN git clone -b ${SPACK_VERSION} https://github.com/spack/spack ${SPACK_DIR} && \
    . ${SPACK_DIR}/share/spack/setup-env.sh && \
    git clone -b dev https://github.com/lukemartinlogan/hermes.git ${HERMES_DEPS_DIR} && \
    # git clone -b dev https://github.com/HDFGroup/hermes.git ${HERMES_DEPS_DIR} && \
    spack repo add ${HERMES_DEPS_DIR}/ci/hermes && \
    mkdir -p ${HERMES_DIR} && \
    spack external find

# Install hermes_shm
RUN . ${SPACK_DIR}/share/spack/setup-env.sh && \
    spack install hermes_shm@master+vfd+mpiio^mpich@3.3.2

# Install jarvis-cd
RUN git clone https://github.com/grc-iit/jarvis-cd.git && \
    cd jarvis-cd && \
    pip install -e . -r requirements.txt

# Install scspkg
RUN git clone https://github.com/grc-iit/scspkg.git && \
    cd scspkg && \
    pip install -e . -r requirements.txt
