from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout
from conan.tools.files import copy
import os


class KafkaToNexusConan(ConanFile):
    name = "kafka-to-nexus"
    version = "6.2.2"
    settings = "os", "arch", "compiler", "build_type"
    generators = "CMakeToolchain", "CMakeDeps"
    exports_sources = "CMakeLists.txt", "src/*", "apps/*", "LICENSE.md"

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

    def layout(self):
        cmake_layout(self)

    def configure(self):
        self.conf_info.define("tools.cmake.cmaketoolchain:generator", "Ninja")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        # Copy kafka-to-nexus license
        copy(self, "LICENSE*", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder, keep_path=False)

        # Copy executables
        apps_bin_dir = os.path.join(self.build_folder, "apps")
        copy(self, "kafka-to-nexus", dst="bin", src=apps_bin_dir, keep_path=False)
        copy(self, "file-maker", dst="bin", src=apps_bin_dir, keep_path=False)
        copy(self, "template-maker", dst="bin", src=apps_bin_dir, keep_path=False)

        # Copy licenses of dependencies
        for dep in self.deps_cpp_info.deps:
            license_dir = os.path.join(self.deps_cpp_info[dep].rootpath, "licenses")
            if os.path.exists(license_dir):
                copy(self, "*", dst=os.path.join(self.package_folder, "licenses", dep), src=license_dir)

        # Copy libs of deps
        for dep in self.deps_cpp_info.deps:
            lib_dir = os.path.join(self.deps_cpp_info[dep].rootpath, "lib")
            if os.path.exists(lib_dir):
                copy(self, "*.so*", dst=os.path.join(self.package_folder, "lib"), src=lib_dir, keep_path=False)
                copy(self, "*.a", dst=os.path.join(self.package_folder, "lib"), src=lib_dir, keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["filewriter_lib"]
