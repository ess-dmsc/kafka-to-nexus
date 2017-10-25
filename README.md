# Kafka to Nexus file writing

- [Usage](#usage)
- [Installation](#installation)
- [Flatbuffer Schema Plugins](#flatbuffer-schema-plugins)


## Features

- What for file writing command from a Kafka topic
- Write data to file
- Writer plugins can be [configured via the json command](#options-for-schema-plugins)
- And more coming up...


## Usage

### Running kafka-to-nexus

```
./kafka-to-nexus -h
```

For example:
```
./kafka-to-nexus --command-uri //kafka-host/filewriter-commands
```


### Configuration File

The file writer can be configured via `--config-file <json>`

Available options include:

```
{
  "command-uri": "//broker[:port]/command-topic",
  "status-uri": "//broker[:port]/status-topic",
  "status-master-interval": 2000,
  "commands": [
    "a list of commands as discussed below."
  ]
}
```

- `command-uri` Kafka URI where the file writer listens for commands
- `status-uri` Kafka URI where to publish status updates
- `status-master-interval` Interval in milliseconds for status updates


### Send command to kafka-to-nexus

Commands in the form of JSON messages are used to start and stop file writing.
Commands can be send through Kafka via the broker/topic specified by the
`--command-uri` option.  Commands can also be given in the configuration
file specified by `--config-file <file.json>` (see a bit later in this
section).

In the command, the `nexus_structure` defines the HDF hierarchy.
The `nexus_structure` represents the HDF root object.  The following example
shows how the HDF tree can be constructed using `children`.
A child of type `stream` is a marker that some `HDFWriterModule` will insert
the data from a Kafka stream at that point in the hierarchy.  The options under
the key `stream` specify the details, at least the topic, the source name and
the `HDFWriterModule` which should be used for writing.
Depending on the `HDFWriterModule`, there will be more options specific to the
`HDFWriterModule`.

Command to start writing a file:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "for_example_motor_0000",
        "attributes": {
          "NX_class": "NXinstrument"
        },
        "children": [
          {
            "type": "stream",
            "attributes": {
              "this_will_be_a_double": 0.123,
              "this_will_be_a_int64": 123
            },
            "stream": {
              "topic": "topic.with.multiple.sources",
              "source": "for_example_motor",
              "module": "f142",
              "type": "float",
              "array_size": 4
            }
          },
          {
            "type": "dataset",
            "name": "some_static_dataset",
            "values": 42.24,
            "attributes": {
              "units": "Kelvin"
            }
          },
          {
            "type": "dataset",
            "name": "some_more_explicit_static_dataset",
            "dataset": {
              "space": "simple",
              "type": "uint64",
              "size": ["unlimited", 5, 6]
            },
            "values": [[[0, 1, 2, 3, 4, 5], [...], ...], [...], ...]
          }
        ]
      }
    ]
  },
  "file_attributes": {
    "file_name": "some.h5"
  },
  "cmd": "FileWriter_new",
  "jobid" : "16char-unique-identifier",
}
```

Command to exit the file writer:

```json
{"cmd": "FileWriter_exit"}
```

Commands can be given in the configuration file as well:

```json
{
  "commands": [
    { "some command": "as discussed above" }
  ]
}
```



### Options for the f142 writer module

- `type`: The data type contained in the flat buffer. Can be `int8` to `int64`,
  similar for `uint8`, `float` and `double`.
- `array_size`: The size of the array. Scalar if not specified or `0`.


## Installation

### Requirements

- cmake (at least 2.8.11)
- git
- flatbuffers (headers and working `flatc`)
- librdkafka
- rapidjson
- hdf5
- libfmt (e.g. `yum install fmt fmt-devel` or `brew install fmt`)
- `streaming-data-types` repository (clone e.g. so that both `kafka-to-nexus`
  and `streaming-data-types` are in the same directory)
- Optional `graylog_logger`

Tooling

- conan
- cmake (minimum tested is 2.8.11)
- C++ compiler with c++11 support
- Doxygen if you would like to make docs

### Conan repositories

The following remote repositories are required to be configured:

- https://api.bintray.com/conan/ess-dmsc/conan
- https://api.bintray.com/conan/conan-community/conan

You can add them by running
```
conan remote add <local-name> <remote-url>
```
where `<local-name>` must be substituted by a locally unique name. Configured
remotes can be listed with `conan remote list`.

### Build

As usual `cmake`, `make`.
```
conan install <path-to-source>/conan --build=missing
cmake <path-to-source> [-DREQUIRE_GTEST=TRUE]
make
make docs  # optional
```

#### Usage of your custom builds of the dependencies

If you have dependencies in non-standard locations:
Locations of dependencies can be supplied via the standard
`CMAKE_INCLUDE_PATH` and `CMAKE_LIBRARY_PATH` variables.

- `flatbuffers` Headers plus `flatc`, therefore set `CMAKE_INCLUDE_PATH` and `CMAKE_PROGRAM_PATH`.

- `rapidjson`

- `HDF5`

- `graylog_logger` Additionally, set `USE_GRAYLOG_LOGGER=1`
  - cmake will report if it is found

- `libfmt` Header/Source-only
  - we expect `fmt/[format.cc, format.h]`

- `Google Test` (optional) Easiest way: `git clone https://github.com/google/googletest.git`
  in parallel to this repository, or give the repository location in
  `CMAKE_INCLUDE_PATH` or in `GOOGLETEST_REPOSITORY_DIR`.
  Enable gtest usage by `REQUIRE_GTEST=1`

If you like full fine-grained control over the locations, you can of course set
the locations directly as the package-specific variables which can be looked up
in the `Find...` scripts under `./cmake/` for each package.


### Using Ansible

Install using the playbook:

```
ansible-playbook -i hosts kafka-to-nexus.yml
```

The filewriter can be installed using the ansible playbook defined in
`ansible`. The file `roles/kafka-to-nexus/defaults/main.yml` defines
the variables used during installation. The variables `<dep>_src` and
`<dep>_version` are the remote source and the required version of the
dependency, `<dep>` the install location. The sources and builds of
the dependencies are kept in `sources` and `builds`.

`filewriter_inc`, `filewriter_lib` and `filewriter_bin` defines
`CMAKE_INCLUDE_PATH`, `CMAKE_LIBRARY_PATH` and `CMAKE_PROGRAM_PATH`.

The default installation has the following structure
```
/opt/software/sources/<package1>-<version>
/opt/software/sources/<package2>-<version>
...
/opt/software/builds/<package1>-<version>
/opt/software/builds/<package2>-<version>
...
/opt/software/<package1>-<version>
/opt/software/<package2>-<version>
...
/opt/software/sources/filewriter-<version>
/opt/software/builds/filewriter-<version>
```


## Flatbuffer Schema Plugins

The actual parsing of the different FlatBuffer schemata is handled by plugins
which register themselves via the `FileWriter::FlatbufferReaderRegistry` and
`FileWriter::HDFWriterModuleRegistry`.  See for example
`kafka-to-nexus/src/schemas/ev42/ev42_rw.cxx` and search for `Registrar` in
the code.  Support for new schemas can be added in the same way.


### Options for Schema Plugins

Schema plugins can access configuration options which got passed via the
`FileWriter_new` json command.  The writer implementation, meaning the class
which derives from `FBSchemaWriter`, can read its member variable
`rapidjson::Document const * config_stream` which contains all the options.

In this example, we assume that a stream uses the `f142` schema.  We tell the
writer for the `f142` schema to write an index entry every 3 megabytes:
```
{
  "cmd": "FileWriter_new",
  "broker": "<your-broker>",
  "streams": [
    {
      "topic": "<kafka-topic>",
      "source": "<some-source-using-schema-f142>",
      "nexus_path": "/path/to/the/nexus/group",
      "nexus": {
        "indices": {
          "index_every_mb": 3
        }
      }
    }
  ], ...
```


### Available Options for Schema Plugins

Not that many in this release, but will be extended with upcoming changes:

- `f142`
  - `nexus.indices.index_every_mb`
    Write an index entry (in Nexus terminology: cue entry) every given
    megabytes.


## Running Tests

Tests are built only when `gtest` is detected.  If detected, the `cmake` output
contains
```
-- Using Google Test: [ DISCOVERED_LOCATION_OF_GTEST ]
```
with the location where it has found `gtest`.

Start the `gtest` based test suite via:
```
./tests/tests
```


## Performance

- [Profiling ev42 HDF writer module](docs/profile-ev42.md)


## Documents:

[Nexus-for-ESS](https://confluence.esss.lu.se/display/DMSC/NeXus+for+ESS)

[file-writer-2016-10-28](https://confluence.esss.lu.se/download/attachments/48202445/BrightNeXus.pdf?version=1&modificationDate=1477659873237&api=v2)


## Archive

These documents are outdated regarding technical documentation, but linked here
for archival:

- [NeXusFileWriterDesign](docs/NeXusFileWriterDesign.md) (Mark).


## Graph of dependencies and data flow

 Very early draft so far:

![Flow](flow.svg)

A pictorial representation of the implementation is ![File Writer overall design](docs/FileWriter.jpg)

## Streamer

According to the design the Streamer connects to Kafka (other
sources to be implemented) and consumes a message in the specified topic. Some features:
* one Streamer per topic
* multiple Source per streamer
* initial timestamp is specified using ``set_start_time``
* connection to the Kafka broker is nonblocking. If the broker address is invalid returns an error
* Kafka::Config and streamer options can be passed using the constructor ``kafka_options`` and ``filewriter_options`` respectively.

## DemuxTopic
Mapped 1:1 with topics (and Streamers) drives the message to the correct Source. Derived from classes MessageProcessor and TimeDifferenceFromMessage. The former provides an interface for processing new messages (usually write on disk), the latter the interface process old messaged with the aim of find the first message sent after ECP ```start ```message.
The two corresponding methods are
* process_message
* time_difference_from_message
Both receive the message payload and size. Return values are ProcessMessageResult and TimeDifferenceFromMessage_DT.

## Source

## StreamMaster 

The StreamMaster receives the array of DemuxTopic from
FileWriterCommand and instantiates the Streamer array according to the
topics. Eventually retrieves the list of brokers from Kafka.

* `start_time` and `stop_time` can be used to set the timestamp of
the first and last event to be written (caution: actually events
with timestamp earlier than request will be written);
* upon a `stop` message the ``Master`` can stop the writing;
* if a `status-uri` is configured sends a (JSON formatted) status
  report on the corresponding topic;
* a global `status` flag report the status of
``StreamMaster``. Definitions are in
`Status::StreamMasterErrorCode` (the function `Err2Str`
converts the error code into a human readable string). 


More tests involving the network:
```
tests/streamer_test --kafka_broker=<broker>:<port>  --kafka_topic="<topic name>"
tests/streammaster_test --kafka_broker=<broker>:<port>"
```
Tests are implemented using the gtest suite. They support all the command
line option provided by gtest.
