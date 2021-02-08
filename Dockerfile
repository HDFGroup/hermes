FROM ubuntu:18.04

ENV USER=hermes
RUN useradd -ms /bin/bash $USER
RUN su - $USER -c "touch me"

RUN apt-get update -q && \
    apt-get install -yq gcc g++

RUN apt-get install -y sudo
RUN echo "user ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USER && \
    chmod 0440 /etc/sudoers.d/$USER

RUN DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
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
    cmake

CMD ["su", "-", "$USER", "-c", "/bin/bash"]

USER $USER

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

RUN echo $GITPOD_REPO_ROOT

RUN $spack repo add ${SDS_DIR}
RUN $spack repo add $PROJECT/ci/hermes

RUN $spack compiler find

RUN $spack compiler list

ENV HERMES_VERSION=master

ENV HERMES_SPEC="hermes@master"
RUN $spack install ${HERMES_SPEC}

RUN $spack view --verbose symlink ${INSTALL_DIR} ${HERMES_SPEC}

RUN mkdir $PROJECT/build
ENV CXXFLAGS="${CXXFLAGS} -std=c++17 -Werror -Wall -Wextra"
ENV PATH="${INSTALL_DIR}/bin:${PATH}"
RUN cd $PROJECT/build && \
    cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR} \
            -DCMAKE_PREFIX_PATH=${INSTALL_DIR}                  \
        -DCMAKE_BUILD_RPATH=${INSTALL_DIR}/lib              \
        -DCMAKE_INSTALL_RPATH=${INSTALL_DIR}/lib            \
        -DCMAKE_BUILD_TYPE=Release                       \
        -DCMAKE_CXX_COMPILER=`which mpicxx`                    \
        -DCMAKE_C_COMPILER=`which mpicc`                       \
        -DBUILD_SHARED_LIBS=ON                                 \
        -DHERMES_ENABLE_COVERAGE=ON                            \
        -DHERMES_INTERCEPT_IO=ON                               \
        -DHERMES_COMMUNICATION_MPI=ON                          \
        -DHERMES_BUILD_BUFFER_POOL_VISUALIZER=OFF               \
        -DORTOOLS_DIR=${INSTALL_PREFIX}                        \
        -DHERMES_USE_ADDRESS_SANITIZER=ON                      \
        -DHERMES_USE_THREAD_SANITIZER=OFF                      \
        -DHERMES_RPC_THALLIUM=ON                               \
        -DHERMES_DEBUG_HEAP=OFF                                \
        -DBUILD_TESTING=ON                                     \
        ..

RUN cd $PROJECT/build && \
    cmake --build . -- -j4
RUN cd $PROJECT/build && \
    ctest -VV

WORKDIR $HOME