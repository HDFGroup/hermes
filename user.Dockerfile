FROM hdevarajan92/hermes-dep:latest

RUN sudo apt-get update -q

USER $USER

ENV HOME=/home/$USER

ENV INSTALL_DIR=$HOME/install

ENV PROJECT=$HOME/hermes

RUN $spack install --only package ${HERMES_SPEC}

RUN echo "spack load ${HERMES_SPEC}" >> /home/$USER/.bashrc
RUN echo "export CC=`which mpicc`" >> /home/$USER/.bashrc
RUN echo "export CXX=`which mpicxx`" >> /home/$USER/.bashrc

RUN sh /home/$USER/.bashrc

WORKDIR $HOME