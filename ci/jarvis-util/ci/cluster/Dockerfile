# Install ubuntu 20.04
FROM ubuntu:20.04
LABEL maintainer="llogan@hawk.iit.edu"
LABEL version="0.0"
LABEL description="An example docker image"

# Disable Prompt During Packages Installation
ARG DEBIAN_FRONTEND=noninteractive

# Update ubuntu
RUN apt update && apt install

# Install some basic packages
RUN apt install -y \
    openssh-server \
    sudo git nano vim \
    docker \
    mpich \
    gcc \
    g++ \
    gfortran \
    libtool \
    libtool-bin \
    automake \
    autoconf

# Create a new user
RUN useradd -m sshuser
RUN usermod -aG sudo sshuser
RUN passwd -d sshuser

# Copy the host's SSH keys
# Docker requires COPY be relative to the current working
# directory. We cannot pass ~/.ssh/id_rsa unfortunately...
ENV SSHDIR=/home/sshuser/.ssh
RUN sudo -u sshuser mkdir ${SSHDIR}
COPY id_rsa ${SSHDIR}/id_rsa
COPY id_rsa.pub ${SSHDIR}/id_rsa.pub

# Authorize host SSH keys
RUN sudo -u sshuser touch ${SSHDIR}/authorized_keys
RUN cat ${SSHDIR}/id_rsa.pub >> ${SSHDIR}/authorized_keys

# Set SSH permissions
RUN chmod 700 ${SSHDIR}
RUN chmod 644 ${SSHDIR}/id_rsa.pub
RUN chmod 600 ${SSHDIR}/id_rsa
RUN chmod 600 ${SSHDIR}/authorized_keys

# Enable passwordless SSH
RUN sed -i 's/#PermitEmptyPasswords no/PermitEmptyPasswords yes/' /etc/ssh/sshd_config

# Start SSHD and wait forever
RUN mkdir /run/sshd
CMD ["/usr/sbin/sshd", "-D"]