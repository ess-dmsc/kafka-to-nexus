FROM ubuntu:18.04

ARG http_proxy

ARG https_proxy

ARG local_conan_server

# Replace the default profile and remotes with the ones from our Ubuntu build node
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/registry.json" "/root/.conan/registry.json"
ADD "https://raw.githubusercontent.com/ess-dmsc/docker-ubuntu18.04-build-node/master/files/default_profile" "/root/.conan/profiles/default"
COPY ./conan ../kafka_to_nexus_src/conan

# Install packages - We don't want to purge kafkacat and tzdata after building
# boost_regex package fails to build with more recent versions of conan so we're using 1.12.1
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update -y \
    && apt-get --no-install-recommends -y install build-essential git python python-pip cmake python-setuptools autoconf libtool automake kafkacat tzdata \
    && apt-get -y autoremove  \
    && apt-get clean all \
    && rm -rf /var/lib/apt/lists/* \
    && pip install conan==1.12.1 \
    && mkdir kafka_to_nexus \
    && if [ ! -z "$local_conan_server" ]; then conan remote add --insert 0 ess-dmsc-local "$local_conan_server"; fi \
    && cd kafka_to_nexus \
    && conan install --build=outdated ../kafka_to_nexus_src/conan/conanfile.txt

COPY ./ ../kafka_to_nexus_src/

RUN cd kafka_to_nexus \
    && cmake -DCONAN="MANUAL" --target="kafka-to-nexus" -DCMAKE_BUILD_TYPE=Release -DUSE_GRAYLOG_LOGGER=True -DRUN_DOXYGEN="FALSE" ../kafka_to_nexus_src \
    && make -j8 \
    && mkdir /output-files \
    && conan remove "*" -s -f \
    && apt purge -y build-essential git python python-pip cmake python-setuptools autoconf libtool automake \
    && mv ../../kafka_to_nexus_src/docker_launch.sh /docker_launch.sh \
    && rm -rf ../../kafka_to_nexus_src/* \
    && rm -rf /tmp/* /var/tmp/* /kafka_to_nexus/src /root/.conan/

CMD ["/docker_launch.sh"]
