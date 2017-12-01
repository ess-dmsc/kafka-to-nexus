FROM ubuntu:17.04

# Install packages
ENV BUILD_PACKAGES "build-essential git python python-pip cmake python-setuptools"
RUN apt-get -y update && apt-get install $BUILD_PACKAGES -y --no-install-recommends && \
rm -rf /var/lib/apt/lists/*

RUN pip install conan
RUN conan remote add ess-dmsc https://api.bintray.com/conan/ess-dmsc/conan
RUN conan remote add conan-community https://api.bintray.com/conan/conan-community/conan

# Only add the conan file in this layer,
# otherwise have to rebuild dependencies every time anything in kafka-to-nexus changes
RUN mkdir kafka_to_nexus_src
RUN mkdir kafka_to_nexus_src/conan
ADD ./conan /kafka_to_nexus_src/conan

# Build dependencies
RUN mkdir kafka_to_nexus
RUN cd kafka_to_nexus
RUN conan install ../kafka_to_nexus_src/conan --build=missing -s compiler.libcxx=libstdc++11

ADD . ../kafka_to_nexus_src

RUN git config --global http.sslVerify false && \
    git clone https://github.com/ess-dmsc/streaming-data-types.git

RUN cd kafka_to_nexus && \
    mv ../conanbuildinfo.cmake conanbuildinfo.cmake && \
    cmake ../kafka_to_nexus_src && \
    make -j8

RUN mkdir /output-files
ADD manual-testing/docker_launch.sh /

CMD ["./docker_launch.sh"]
