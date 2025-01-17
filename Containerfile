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

COPY . . 

RUN python3 -m venv .venv
RUN . .venv/bin/activate && pip install "conan<2" && conan profile new default --detect && conan profile update settings.compiler.libcxx=libstdc++11 default && conan profile update settings.compiler.version=11 default && conan config install http://github.com/ess-dmsc/conan-configuration.git && mkdir _build && cd _build && conan install .. --build=missing
RUN . .venv/bin/activate && cmake .. -DRUN_DOXYGEN=OFF -DHTML_COVERAGE_REPORT=OFF
RUN cmake --build . --parallel

ENTRYPOINT ["bin/kafka-to-nexus"]
