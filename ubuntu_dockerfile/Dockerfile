FROM ubuntu:latest

ENV DEBIAN_FRONTEND noninteractive
RUN apt update && \
    apt -y install build-essential software-properties-common
RUN apt-get update && \
    apt-get -y install make gcc mono-mcs git wget unzip flex bison byacc git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /home
SHELL ["/bin/bash", "-c"]
