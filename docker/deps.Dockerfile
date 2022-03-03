FROM ubuntu:20.04

ENV USER=hermes
RUN useradd -ms /bin/bash $USER
RUN su - $USER -c "touch me"

RUN apt-get update -q --fix-missing && \
    apt-get install -yq gcc g++

RUN apt-get install -y sudo
RUN echo "${USER} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USER && \
    chmod 0440 /etc/sudoers.d/$USER

RUN cat /etc/sudoers.d/$USER

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    ca-certificates \
    curl \
    environment-modules \
    git \
    build-essential \
    python \
    python-dev \
    python3-dev \
    vim \
    sudo \
    unzip \
    cmake \
    lcov \
    zlib1g-dev \
    libsdl2-dev \
    gfortran \
    graphviz \
    doxygen

USER $USER

RUN sudo apt-get update -q

ENV HOME=/home/$USER

ENV PROJECT=$HOME/source
ENV INSTALL_DIR=$HOME/install
ENV SPACK_DIR=$HOME/spack
ENV MOCHI_DIR=$HOME/mochi

RUN echo $INSTALL_DIR && mkdir -p $INSTALL_DIR

RUN git clone https://github.com/spack/spack ${SPACK_DIR}
RUN git clone https://github.com/mochi-hpc/mochi-spack-packages.git ${MOCHI_DIR}
RUN git clone https://github.com/HDFGroup/hermes ${PROJECT}

ENV spack=${SPACK_DIR}/bin/spack

RUN . ${SPACK_DIR}/share/spack/setup-env.sh

RUN $spack repo add ${MOCHI_DIR}
RUN $spack repo add $PROJECT/ci/hermes

RUN $spack compiler find

RUN $spack compiler list

ENV HERMES_VERSION=master

ENV HERMES_SPEC="hermes@${HERMES_VERSION}"
RUN $spack install --only dependencies ${HERMES_SPEC}

RUN echo "export PATH=${SPACK_DIR}/bin:$PATH" >> /home/$USER/.bashrc
RUN echo ". ${SPACK_DIR}/share/spack/setup-env.sh" >> /home/$USER/.bashrc

ENV PATH=${INSTALL_DIR}/bin:$PATH

WORKDIR $HOME
