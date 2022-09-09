FROM ubuntu:20.04
ARG  DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt-get install -y autoconf
RUN apt-get install -y automake
RUN apt-get install -y libtool
RUN apt-get install -y libtool-bin
RUN apt-get install -y mpich
RUN apt-get install -y lcov
RUN apt-get install -y zlib1g-dev
RUN apt-get install -y libsdl2-dev
RUN apt-get install -y python3 python3-pip
RUN apt-get install -y git cmake
RUN python3 -m pip install cpplint==1.5.4

#RUN git clone https://github.com/lukemartinlogan/hermes.git -b yaml-conf
RUN git clone https://github.com/HDFGroup/hermes.git
WORKDIR hermes
RUN git remote get-url origin
RUN ci/install_deps.sh
RUN ci/install_hermes.sh
