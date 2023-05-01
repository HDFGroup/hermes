FROM ubuntu:20.04
ARG  DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt-get install -y --no-install-recommends  \
    gcc g++ gfortran \
    autoconf \
    automake \
    libtool \
    libtool-bin \
    mpich \
    lcov \
    zlib1g-dev \
    libsdl2-dev \
    python3 \
    python3-pip \
    git \
    cmake
RUN python3 -m pip install cpplint==1.5.4

RUN git clone https://github.com/HDFGroup/hermes
WORKDIR $PWD/hermes
RUN git remote get-url origin
RUN ci/install_deps.sh
RUN ci/install_hermes.sh
