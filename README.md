[![pipeline status](https://gitlab.esss.lu.se/ecdc/ess-dmsc/kafka-to-nexus/badges/main/pipeline.svg)](https://gitlab.esss.lu.se/ecdc/ess-dmsc/kafka-to-nexus/-/commits/main)
[![coverage report](https://gitlab.esss.lu.se/ecdc/ess-dmsc/kafka-to-nexus/badges/main/coverage.svg)](https://gitlab.esss.lu.se/ecdc/ess-dmsc/kafka-to-nexus/-/commits/main)
[![DOI](https://zenodo.org/badge/81435658.svg)](https://zenodo.org/badge/latestdoi/81435658)


# Kafka to Nexus

Writes NeXus files from experiment data streamed through Apache Kafka.
Part of the ESS data streaming pipeline.

## Applications
The file-writer package consists of a number of applications:
- kafka-to-nexus - this is the core application used for writing NeXus files from information supplied via Kafka
- template-maker - this application is used for generating a template NeXus file from a JSON template
- file-maker - this application allows NeXus files to be written without needing Kafka. It is mostly for debugging and testing.

All applications are run from the command-line. Run any application with the `--help` flag for more details about how to run the application.

## Building the applications
The following minimum software is required to get started:

- Linux or MacOS
- Conan < 2.0.0
- CMake >= 3.1.0
- Git
- A C++17 compatible compiler 
- Doxygen (only required if you would like to generate the documentation)

Conan is a package manager and, thus, will install all the other required packages.

### Configuring Conan
We have our own Conan repositories for some of the packages required.
Follow the README [here](https://github.com/ess-dmsc/conan-configuration) for instructions on how to include these.

Depending on your system, you might need to run the following command.
However, try without first.
```bash
# Assuming profile is named "default"
conan profile update settings.compiler.libcxx=libstdc++11 default
```

### Building the applications
From within the top-most directory:

```bash
mkdir _build
cd _build
conan install .. --build=missing
cmake ..
make
```

There are additional optional CMake flags for adjusting the build:
* `-DRUN_DOXYGEN=ON` if Doxygen documentation is required. Also, requires `make docs` to be run afterwards
* `-DHTML_COVERAGE_REPORT=ON` to generate a html unit test coverage report, output to `<BUILD_DIR>/coverage/index.html`

## Tests
We have three levels of tests:
- unit tests: typically low-level tests
- domain tests: mid-level tests that exercise the codebase but without Kafka
- integration tests: high level tests that confirm that the basic function of the application including Kafka

### Building and running the unit tests
From the build directory:

```bash
make UnitTests
./bin/UnitTests
```

Note: some of the tests may fail on MacOS but as Linux is our production system this doesn't matter. 
Instead the tests can be run on the build server - if they fail there then there is definitely a problem!

### Running the domain tests
These tests run the core functionality of the program but without Kafka. This means the tests are more stable than the integration
tests as they do not require Docker and Kafka to be running.

The domain tests are implemented in Python but use the file-maker to generate a file. 

Inside the domain tests folder there is a `requirements.txt` file; these requirements need to be install before running the domain tests.
```bash
pip install -r requirements.txt
```
The tests can be run like so:
```
$ cd domain-tests
$ pytest --file-maker-binary=<path to file-maker binary>
```
The "data" for writing is specified via JSON; for an example see`domain-tests/data_file.json`. 
Correspondingly, there is a NeXus template file that defines the layout of the produced file. 
See `domain-tests/nexus_template.json` for an example.

The tests work by first generating a NeXus file using the file-maker and then run tests against the file to check it is correct.

### Running the integration tests
The integration tests consist of a series of automated tests for this repository that test it in ways similar to how it would
be used in production.

See [Integration Tests page](integration-tests/README.md) for more information.

## The file-writer
The file-writer can be configured from a file via `--config-file <ini>` which mirrors the command line options (use flag `--help` to see them).

There are many options but most have sensible defaults. The minimum ones that need setting are:
```
brokers=localhost:9092
command-status-topic=local_filewriter_status
job-pool-topic=local_filewriter_pool
hdf-output-prefix=output
```
- brokers is the addresses of the Kafka brokers comma-separated.
- command-status-topic is where the file-writer sends status messages when it is NOT writing.
- job-pool-topic is the topic where the file-writer receives request for writing files. This mechanism is explained below.
- hdf-output-prefix defines the path where the files should be written.

### The job pool
The file-writer is designed to run in a system where multiple other file-writers are present.
When a request to start writing a file is sent to the job pool one of the file-writers will pick it up.
That file-writer will switch to a new topic specified via start message. It will then send a message indicating that it has started 
writing to that topic. The request to stop writing will also come to that topic.
Once the file-writer has finished writing it will disconnect from the topic and return to the job pool topic.

Note: all file-writers belong to the same consumer group, so only one will consumer the start message.

This [flow-chart](documentation/file-writer-command-handler-state-machine.png) shows this information pictorially.

For more information on the commands send to and from the file-writer see [commands](documentation/commands.md).

## The template-maker
To reduce the size of the start messages sent to Kafka, it is possible to use pre-created template NeXus files.
These usually contain the static (rarely changed) information for a particular instruments, e.g. the geometry of the instrument.
When file-writing starts, the template file is copied and then populated with the dynamic data.
Each instrument has its own unique template. 

The source of truth for the static and dynamic configurations is a large JSON file. The template-maker generates
the configurations from this file and then the templates are deployed for use.

If there is a change to the JSON then the templates need to be recreated and redeployed.

Use the `--help` flag to see how to use it.

## The file-maker
Similar to the template-maker, the file-maker allows creating a NeXus file without Kafka. The data and file configuration
are read from files.

Its main purpose is for allowing developers to do testing and debugging in a more deterministic way by having consistent
input data and not having to set up Kafka.

The file-maker has a fixed start and stop time (10000ms to 15000ms Unix epoch), so the data supplied needs to take that into account.
Note: times in the data file is in ms, the file-maker will convert them to the correct units automatically.

Use the `--help` flag to see how to use it. The domain-tests also use it, so they are a good way to learn more about it.

## Further documentation

See the `documentation` directory.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for information on submitting code to this project.

## License

This project is licensed under the BSD 2-Clause "Simplified" License - see the [LICENSE.md](LICENSE.md) file for details.
