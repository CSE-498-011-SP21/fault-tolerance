FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN apt update -y
RUN apt upgrade -y
RUN apt install -y cmake build-essential git curl zip unzip tar pkg-config wget bzip2
RUN apt install -y libboost-all-dev libtbb-dev gdb

# RUN apt update && apt install -y cmake build-essential git curl zip unzip tar pkg-config wget bzip2

RUN wget https://github.com/ofiwg/libfabric/releases/download/v1.9.1/libfabric-1.9.1.tar.bz2 && \
    bunzip2 libfabric-1.9.1.tar.bz2 && tar xf libfabric-1.9.1.tar && cd libfabric-1.9.1 && ./configure && \
    make -j && make install

COPY . /fault-tolerance

WORKDIR /fault-tolerance

EXPOSE 8080