FROM hdfgroup/hermes-deps:latest

RUN sudo apt-get update -q

USER $USER

ENV HOME=/home/$USER

ENV INSTALL_DIR=$HOME/install

ENV PROJECT=$HOME/hermes

RUN $spack install --only package ${HERMES_SPEC}

RUN echo "spack load --only dependencies ${HERMES_SPEC}" >> /home/$USER/.bashrc
RUN echo "export CC=`which mpicc`" >> /home/$USER/.bashrc
RUN echo "export CXX=`which mpicxx`" >> /home/$USER/.bashrc

RUN git clone https://github.com/HDFGroup/hermes ${PROJECT}
RUN sh /home/$USER/.bashrc

RUN mkdir -p $PROJECT/build
WORKDIR $PROJECT/build
