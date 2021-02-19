FROM ubuntu:18.04

ENV USER=hermes
RUN useradd -ms /bin/bash $USER
RUN su - $USER -c "touch me"

RUN apt-get update -q && \
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
    vim \
    sudo \
    unzip \
    cmake \
    lcov \
    zlib1g-dev \
    libsdl2-dev \
    gfortran

USER $USER

RUN sudo apt-get update -q

ENV HOME=/home/$USER

ENV PROJECT=$HOME/source
ENV INSTALL_DIR=$HOME/install
ENV SPACK_DIR=$HOME/spack
ENV SDS_DIR=$HOME/sds

RUN echo $INSTALL_DIR && mkdir -p $INSTALL_DIR

RUN git clone https://github.com/spack/spack ${SPACK_DIR}
RUN git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git ${SDS_DIR}
RUN git clone https://github.com/HDFGroup/hermes ${PROJECT}


ENV spack=${SPACK_DIR}/bin/spack

RUN . ${SPACK_DIR}/share/spack/setup-env.sh

RUN $spack repo add ${SDS_DIR}
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
