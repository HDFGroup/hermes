FROM hdfgroup/hermes-deps:latest

RUN sudo apt-get update -q

USER $USER

ENV HOME=/home/$USER

ENV INSTALL_DIR=$HOME/install

ENV PROJECT=$HOME/hermes

RUN $spack install gotcha@develop
RUN $spack install mochi-thallium~cereal@0.8.3
RUN $spack install catch2@2.13.3
RUN $spack install gortools@7.7
RUN $spack install mpich@3.3.2~fortran

#RUN echo "spack load --only dependencies ${HERMES_SPEC}" >> /home/$USER/.bashrc
RUN echo "spack load gotcha@develop" >> /home/$USER/.bashrc
RUN echo "spack load mochi-thallium~cereal@0.8.3" >> /home/$USER/.bashrc
RUN echo "spack load catch2@2.13.3" >> /home/$USER/.bashrc
RUN echo "spack load gortools@7.7" >> /home/$USER/.bashrc
RUN echo "spack load mpich@3.3.2~fortran" >> /home/$USER/.bashrc
RUN echo "export CC=`which mpicc`" >> /home/$USER/.bashrc
RUN echo "export CXX=`which mpicxx`" >> /home/$USER/.bashrc

#RUN echo password | passwd $USER --stdin

RUN git clone https://github.com/HDFGroup/hermes ${PROJECT}
RUN sh /home/$USER/.bashrc

RUN mkdir -p $PROJECT/build
WORKDIR $PROJECT/build