FROM quay.io/pypa/manylinux2010_x86_64

RUN yum update

RUN mkdir /io
WORKDIR /io

COPY . .

RUN ls python/ -alh
RUN ./build_wheels.sh