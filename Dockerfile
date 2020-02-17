FROM screamingudder/alpine-build-node:2.0.0

ARG http_proxy
ARG https_proxy
ARG local_conan_server

USER root
WORKDIR /home/root

COPY ./conan kafka_to_nexus_src/conan

# Dirty hack to override flatbuffers/1.11.0 with 1.10 as a bug means 1.11 doesn't build on alpine
# Fixed at HEAD, so can be removed when flatbuffers 1.12 is released
RUN sed -i '10iflatbuffers/1.10.0@google/stable' kafka_to_nexus_src/conan/conanfile.txt

# Install kafkacat to use in detecting when Kafka broker is ready
RUN apk add --no-cache kafkacat

# Explicit build of boost_build required because otherwise we get a version of b2 built against glibc (alpine instead has musl)
RUN if [ ! -z "$local_conan_server" ]; then conan remote add --insert 0 ess-dmsc-local "$local_conan_server"; fi \
    && mkdir kafka_to_nexus \
    && cd kafka_to_nexus \
    && conan install "boost_build/1.69.0@bincrafters/stable" --build \
    && conan install --build=outdated ../kafka_to_nexus_src/conan/conanfile.txt

COPY ./cmake kafka_to_nexus_src/cmake
COPY ./src kafka_to_nexus_src/src
COPY ./CMakeLists.txt kafka_to_nexus_src/CMakeLists.txt

RUN cd kafka_to_nexus \
    && cmake -GNinja -DCONAN="MANUAL" -DCMAKE_BUILD_TYPE=Release -DUSE_GRAYLOG_LOGGER=True -DRUN_DOXYGEN=False -DBUILD_TESTS=False ../kafka_to_nexus_src \
    && ninja kafka-to-nexus \
    && mkdir /output-files \
    && conan remove "*" -s -f \
    && rm -rf ../../kafka_to_nexus_src/* \
    && rm -rf /tmp/* /var/tmp/* /kafka_to_nexus/src /root/.conan/

COPY ./docker_launch.sh ../docker_launch.sh
CMD ["/docker_launch.sh"]
