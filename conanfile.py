import os
from conans import ConanFile

class KafkaToNexusConan(ConanFile):
    name = "kafka-to-nexus"
    version = "0.1"
    settings = "os", "arch", "compiler", "build_type"
    generators = "CMakeToolchain", "CMakeDeps"
    exports_sources = "*"

    requires = (
        "spdlog/1.14.1",
        "asio/1.22.1",
        "nlohmann_json/3.10.5",
        "librdkafka/2.8.0",
        "readerwriterqueue/1.0.6",
        "concurrentqueue/1.0.3",
        "date/3.0.1",
        "flatbuffers/1.12.0",
        "streaming-data-types/13dd32d@ess-dmsc/stable",
        "stduuid/1.2.2",
        "gtest/1.15.0",
        "cli11/2.2.0",
        "trompeloeil/42",
        "h5cpp/0.7.1@ess-dmsc/stable",
        "openssl/3.3.1",
        "fmt/8.1.1"
    )

    default_options = {
        "flatbuffers:shared": True,
        "gtest:shared": False,
        "hdf5:shared": True,
        "h5cpp:with_boost": False,
        "librdkafka:shared": True,
        "librdkafka:ssl": True,
        "librdkafka:sasl": True,
        "date:use_system_tz_db": True
    }

    def configure(self):
        self.conf_info.define("tools.cmake.cmaketoolchain:generator", "Ninja")
