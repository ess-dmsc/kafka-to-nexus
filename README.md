[![Build Status](https://jenkins.esss.dk/dm/job/ess-dmsc/job/kafka-to-nexus/job/master/badge/icon)](https://jenkins.esss.dk/dm/job/ess-dmsc/job/kafka-to-nexus/job/master/)
[![DOI](https://zenodo.org/badge/81435658.svg)](https://zenodo.org/badge/latestdoi/81435658)


# Kafka to Nexus File-Writer

Writes NeXus files from experiment data streamed through Apache Kafka.
Part of the ESS data streaming pipeline.

## Usage

```
   -h,--help                   Print this help message and exit
   --version                   Print application version and exit
   --command-uri URI REQUIRED  <host[:port][/topic]> Kafka broker/topic to listen for commands
   --status-uri URI REQUIRED   <host[:port][/topic]> Kafka broker/topic to publish status updates on
   --graylog-logger-address URI
                               <host:port> Log to Graylog via graylog_logger library
   -v,--verbosity              Set log message level. Set to 0 - 5 or one of
                                 `Trace`, `Debug`, `Info`, `Warning`, `Error`
                                 or `Critical`. Ex: "-v Debug". Default: `Error`
   --hdf-output-prefix TEXT    <absolute/or/relative/directory> Directory which gets prepended to the HDF output filenames in the file write commands
   --logpid-sleep              
   --use-signal-handler        
   --log-file TEXT             Specify file to log to
   --teamid UINT               
   --service-id TEXT           Identifier string for this filewriter instance. Otherwise by default a string containing hostname and process id.
   --list_modules              List registered read and writer parts of file-writing modules and then exit.
   --status-master-interval    Interval in milliseconds for status updates
   --streamer-ms-before-start  Streamer option - milliseconds before start time
   --streamer-ms-after-stop    Streamer option - milliseconds after stop time
   --streamer-start-time       Streamer option - start timestamp (milliseconds)
   --streamer-stop-time        Streamer option - stop timestamp (milliseconds)
   --stream-master-topic-write-interval
                               Stream-master option - topic write interval (milliseconds)
   -S,--kafka-config KEY VALUE ...
                               LibRDKafka options
   -c,--config-file TEXT       Read configuration from an ini file
 
```

### Configuration Files

The file-writer can be configured from a file via `--config-file <ini>` which mirrors the command line options.

For example:

```ini
command-uri=//broker[:port]/command-topic
status-uri=//broker[:port]/status-topic
commands-json=./commands.json
hdf-output-prefix=./absolute/or/relative/path/to/hdf/output/directory
service-id=this_is_filewriter_instance_HOST_PID_EXAMPLENAME
streamer-ms-before-start=123456
kafka-config=consumer.timeout.ms 501 fetch.message.max.bytes 1234 api.version.request true
```

Note: the Kafka options are key-value pairs and the file-writer can be given multiple by appending the key-value pair to 
the end of the command line option.

### Sending commands to the file-writer

Beyond the configuration options given at start-up, the file-writer can be sent commands via Kafka to control the actual file writing.

See [commands](documentation/commands.md) for more information.

## Installation

The supported method for installation is via Conan.

### Prerequisites

The following minimum software is required to get started:

- Conan
- CMake >= 3.1.0
- Git
- A C++17 compatible compiler (preferably GCC or Clang).
GCC >=8 and AppleClang >=10 are good enough; complete C++17 support is not required.
- Doxygen (only required if you would like to generate the documentation)

Conan will install all the other required packages.

### Add the Conan remote repositories

Follow the README [here](https://github.com/ess-dmsc/conan-configuration)

### Build

From within the file-writer's top directory:

```bash
mkdir _build
cd _build
conan install ../conan --build=missing
cmake ..
make
```

There are additional CMake flags for adjusting the build:
* `-DRUN_DOXYGEN=ON` if Doxygen documentation is required. Also, requires `make docs` to be run afterwards
* `-DBUILD_TESTS=OFF` to skip building the unit tests
* `-DHTML_COVERAGE_REPORT=ON` to generate an html unit test coverage report, output to `<BUILD_DIR>/coverage/index.html`

### Running the unit tests

From the build directory:

```bash
./bin/UnitTests
```

### Running on OSX

When using Conan on OSX, due to the way paths to dependencies are handled,
the `activate_run.sh` file may need to be sourced before running the application. The
`deactivate_run.sh` can be sourced to undo the changes afterwards.

### System tests

The system tests consist of a series of automated tests for this repository that test it in ways similar to how it would 
be used in production.

See [System Tests page](system-tests/README.md) for more information.

## Documentation

See the `documentation` directory.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for information on submitting pull requests to this project.

## License

This project is licensed under the BSD 2-Clause "Simplified" License - see the [LICENSE.md](LICENSE.md) file for details.

