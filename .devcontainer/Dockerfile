# syntax=docker/dockerfile:1.3
FROM python:3.9.10-slim-buster

# basic packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get -y update                && \
    apt-get -y upgrade               && \
    apt-get install -y                  \
    software-properties-common      \
    sudo                            \
    build-essential                 \
    wget                            \
    curl                            \
    git                             \
    vim                             \
    unzip                           \
    libpq-dev

RUN apt-get -y update                           && \
    apt-get install -y                             \
    python3-tk                                     \
    python3-distutils                              \
    python3-setuptools


RUN pip3 install --upgrade pip
COPY .devcontainer/requirements.txt .
RUN \
    --mount=type=cache,target=/root/.cache/pip \
    pip3 install \
    -r requirements.txt

RUN apt-get autoremove -y                        && \
    apt-get autoclean -y                         && \
    rm -rf /var/lib/apt/lists/*

#-------------------------------------------------------------
#       Post Processing
#-------------------------------------------------------------

## Switch to non-root user
ENV USER=dock
ENV GROUP=sudo

RUN useradd -ms /bin/bash ${USER} && \
    usermod -aG ${GROUP} ${USER}

# Cleaning Part
USER root
RUN apt-get autoremove -y && \
    apt-get autoclean -y && \
    rm -rf /var/lib/apt/lists/*

# Resolve authorization problem
RUN echo "${USER} ALL=(ALL) NOPASSWD: ALL" \
    >/etc/sudoers.d/${USER} && \
    chmod 0440 /etc/sudoers.d/${USER}

# Final User
USER ${USER}

#-------------------------------------------------------------
#       Repo Installation
#-------------------------------------------------------------
ENV CONTAINER_NAME="singer-tap-hubspot"
ENV WORKDIR_PATH="/workspaces/singer-tap-hubspot"
ENV PYTHONPATH $PYTHONPATH:$WORKDIR_PATH

WORKDIR $WORKDIR_PATH
COPY ./tap_hubspot ./tap_hubspot

RUN mkdir -p data/logs

CMD [ "/bin/bash" ]