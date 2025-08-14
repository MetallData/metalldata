# FROM mcr.microsoft.com/devcontainers/cpp:1-ubuntu-24.04
FROM mcr.microsoft.com/devcontainers/cpp:dev-ubuntu-24.04

RUN apt update && export DEBIAN_FRONTEND=noninteractive 

# Clangd
RUN apt -y install clangd-19 clang-tidy-19 

# OpenMPI
RUN apt -y install openmpi-bin openmpi-doc libopenmpi-dev 

#Python modules
RUN apt -y install python3-pip python3-venv
RUN pip3 install --break-system-packages pandas pyarrow faker 

RUN update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-19 100
RUN update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-19 100