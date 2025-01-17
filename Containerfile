FROM ubuntu:22.04

RUN apt-get update \
    && export DEBIAN_FRONTEND=noninteractive \
    #
    # Install
    && apt-get -y install \
    build-essential \
    cmake \
    cppcheck \
    libsasl2-dev \
    libssl-dev \
    git \
    zip \
    unzip \
    vim \
    python3-pip \
    python-is-python3 \
    python3-venv \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/filewriter/

EXPOSE 9092

COPY . /opt/filewriter

RUN python3 -m venv .venv

RUN . .venv/bin/activate && pip install "conan<2" && conan profile new default --detect && conan profile update settings.compiler.libcxx=libstdc++11 default && conan profile update settings.compiler.version=11 default && conan config install http://github.com/ess-dmsc/conan-configuration.git 

RUN . .venv/bin/activate && mkdir _build && cd _build && conan install .. --build=missing

RUN . .venv/bin/activate && cd _build && cmake .. -DRUN_DOXYGEN=OFF -DHTML_COVERAGE_REPORT=OFF && cmake --build . --parallel

ENTRYPOINT ["_build/bin/kafka-to-nexus"]
