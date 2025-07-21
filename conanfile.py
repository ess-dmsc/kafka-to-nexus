from conan import ConanFile
from conan.tools.cmake import CMake, cmake_layout, CMakeToolchain, CMakeDeps
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

    options = {
        "sanitizer": ["none", "address", "thread", "undefined"],
    }

    default_options = {
        "flatbuffers:shared": True,
        "gtest:shared": False,
        "hdf5:shared": True,
        "h5cpp:with_boost": False,
        "librdkafka:shared": True,
        "librdkafka:ssl": True,
        "librdkafka:sasl": True,
        "date:use_system_tz_db": True,
        "sanitizer": "none",
    }

    def layout(self):
        cmake_layout(self)

    def generate(self):
        self.conf.define("tools.cmake.cmaketoolchain:generator", "Ninja")

        sanitizer = str(self.options.sanitizer)
        flags = ""

        # Set sanitizer flags based on the option
        if sanitizer == "address":
            flags = "-fsanitize=address,undefined -fno-omit-frame-pointer -DSANITIZER_CAN_USE_ALLOCATOR64=0"
        elif sanitizer == "thread":
            flags = "-fsanitize=thread"
        elif sanitizer == "undefined":
            flags = "-fsanitize=undefined -fno-omit-frame-pointer"

        if flags:
            self.conf.append("tools.build:cxxflags", flags)
            self.conf.append("tools.build:cflags", flags)
            self.conf.append("tools.build:sharedlinkflags", flags)
            self.conf.append("tools.build:exelinkflags", flags)

        tc = CMakeToolchain(self)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

        lib_dest = os.path.join(self.build_folder, "lib")
        for dep in self.dependencies.values():
            for libdir in dep.cpp_info.libdirs:
                copy(self, "*.so*", libdir, lib_dest)
                copy(self, "*.dylib", libdir, lib_dest)

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
