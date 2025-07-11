from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout
from conan.tools.files import copy
import os


class KafkaToNexusConan(ConanFile):
    name = "kafka-to-nexus"
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

    def generate(self):
        lib_dest = os.path.join(self.build_folder, "lib")
        for dep in self.dependencies.values():
            for libdir in dep.cpp_info.libdirs:
                copy(self, "*.so*", libdir, lib_dest)
                copy(self, "*.dylib", libdir, lib_dest)

    def configure(self):
        self.conf_info.define("tools.cmake.cmaketoolchain:generator", "Ninja")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        # Copy kafka-to-nexus project license
        copy(self, "LICENSE*", dst=os.path.join(self.package_folder, "licenses"),
             src=self.source_folder, keep_path=False)

        # Copy executables from bin/
        bin_dir = os.path.join(self.build_folder, "bin")
        for exe in ["kafka-to-nexus", "file-maker", "template-maker"]:
            copy(self, exe, dst=os.path.join(self.package_folder, "bin"),
                 src=bin_dir, keep_path=False)
            
        # Copy libs
        for pattern in ("*.dylib", "*.so*"):
            copy(self, pattern, dst=os.path.join(self.package_folder, "lib"),
                    src=os.path.join(self.build_folder, "lib"), keep_path=False)

        # Copy all dependency licenses
        for dep in self.deps_cpp_info.deps:
            license_dir = os.path.join(self.deps_cpp_info[dep].rootpath, "licenses")
            if os.path.isdir(license_dir):
                copy(self, "*", dst=os.path.join(self.package_folder, "licenses", dep),
                     src=license_dir, keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["filewriter_lib"]
