[![Build Status](https://jenkins.esss.dk/dm/job/ess-dmsc/job/kafka-to-nexus/job/master/badge/icon)](https://jenkins.esss.dk/dm/job/ess-dmsc/job/kafka-to-nexus/job/master/)
[![DOI](https://zenodo.org/badge/81435658.svg)](https://zenodo.org/badge/latestdoi/81435658)


# Kafka to Nexus File-Writer

Writes NeXus files from experiment data streamed through Apache Kafka.
Part of the ESS data streaming pipeline.

## Usage

```
  -h,--help                   Print this help message and exit
  --version                   Print application version and exit
  --command-status-uri URI REQUIRED
                              <host[:port][/topic]> Kafka broker/topic to listen for commands and to push status updates to.
  --job-pool-uri URI REQUIRED 
  			      <host[:port][/topic]> Kafka broker/topic to listen for jobs
  --graylog-logger-address URI
                              <host:port> Log to Graylog via graylog_logger library
  --grafana-carbon-address URI
                              <host:port> Address to the Grafana (Carbon) metrics service.
  -v,--verbosity              Set log message level. Set to 0 - 5 or one of
                                `Trace`, `Debug`, `Info`, `Warning`, `Error`
                                or `Critical`. Ex: "-v Debug". Default: `Error`
  --hdf-output-prefix TEXT    <absolute/or/relative/directory> Directory which gets prepended to the HDF output filenames in the file write commands
  --log-file TEXT             Specify file to log to
  --service-name TEXT
                              Used to generate the service identifier and as an extra metrics ID string.Will make the metrics names take the form: "kafka-to-nexus.[service-name].*"
  --list_modules              List registered read and writer parts of file-writing modules and then exit.
  --status-master-interval    Interval in milliseconds for status updates
  --streamer-ms-before-start  Streamer option - milliseconds before start time
  --streamer-ms-after-stop    Streamer option - milliseconds after stop time
  --kafka-metadata-max-timeout-seconds
                              Max timeout for kafka metadata calls. Note: metadata calls block the application.
  --kafka-error-timeout-seconds
                              Number of seconds to wait for recovery from kafka error before abandoning stream.
  --data-flush-interval       (Max) amount of time between flushing of data to file, in seconds.
  -X,--kafka-config KEY VALUE ...
                              LibRDKafka options
  --abort-on-uninitialised-stream BOOLEAN=0
                              Writer aborts the whole job if one or more streams are misconfigured and fail to start
  -c,--config-file            Read configuration from an ini file
 
```

### Configuration Files

The file-writer can be configured from a file via `--config-file <ini>` which mirrors the command line options.

For example:

```ini
command-status-uri=//broker[:port]/command-topic
job-pool-uri=//broker[:port]/job-pool-topic
hdf-output-prefix=./absolute/or/relative/path/to/hdf/output/directory
service-name=this_is_filewriter_instance_HOST_PID_EXAMPLENAME
streamer-ms-before-start=123456
kafka-config=consumer.timeout.ms 501 fetch.message.max.bytes 1234 api.version.request true
```

Note: the Kafka options are key-value pairs and the file-writer can be given multiple such by appending the key-value pair to 
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
conan install .. --build=missing
cmake ..
make
```

There are additional CMake flags for adjusting the build:
* `-DRUN_DOXYGEN=ON` if Doxygen documentation is required. Also, requires `make docs` to be run afterwards
* `-DBUILD_TESTS=OFF` to skip building the unit tests
* `-DHTML_COVERAGE_REPORT=ON` to generate a html unit test coverage report, output to `<BUILD_DIR>/coverage/index.html`

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
