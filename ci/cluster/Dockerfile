# Build this image:  docker build -t mpi .

FROM ubuntu:20.04

ENV USER mpirun

ENV DEBIAN_FRONTEND=noninteractive \
    HOME=/home/${USER}

RUN apt-get update -y && \
  apt-get install -y --no-install-recommends \
  sudo \
  apt-utils \
  && apt-get install -y --no-install-recommends \
  openssh-server \
  gcc \
  g++ \
  libfabric-dev \
  mpich \
  binutils \
  && apt-get clean && apt-get purge && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN mkdir /var/run/sshd
RUN echo 'root:${USER}' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

# ------------------------------------------------------------
# Add an 'mpirun' user
# ------------------------------------------------------------

ARG USER_ID
ARG GROUP_ID

RUN addgroup --gid ${GROUP_ID} ${USER}
RUN adduser --disabled-password --gecos "" --uid ${USER_ID} --gid ${GROUP_ID} ${USER} && \
    echo "${USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# ------------------------------------------------------------
# Set-Up SSH with our Github deploy key
# ------------------------------------------------------------

ENV SSHDIR ${HOME}/.ssh/

RUN mkdir -p ${SSHDIR}
RUN echo "StrictHostKeyChecking no" > ${SSHDIR}/config

RUN chmod -R 600 ${SSHDIR}* && \
    chown -R ${USER}:${USER} ${SSHDIR}

CMD ["/usr/sbin/sshd", "-D"]
