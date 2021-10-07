# Changes

## Version 4.1.0: Quality of life

- It is no longer possible to set a stop time if the previously set stop time has passed. Also added unit tests for this feature.
- The application will now give you an approximate size of the file it is writing to, rounded up to the nearest 10 MB, in the status messages that it produces.

## Version 4.0.0: Many, many changes

- Messages with the same timestamp, in the FlatBuffer, as the previous message are not written to file.
This feature is turned on for all writer modules except event messages (`ev42`). This allows the Filewriter
to ignore repeated updates from the Forwarder, which are sent to ensure there is an update available on Kafka
from shortly before the start of each file being written. ([#551](https://github.com/ess-dmsc/kafka-to-nexus/pull/551))
- The application is no longer (completely) blocking when doing initial set-up kafka meta data calls. ([#554](https://github.com/ess-dmsc/kafka-to-nexus/pull/554))
- Fixed bug which caused f142 messages with value of zero to be written as a different value. ([#556](https://github.com/ess-dmsc/kafka-to-nexus/pull/556))
- Updated conan package dependencies ([#557](https://github.com/ess-dmsc/kafka-to-nexus/pull/557)):
  - librdkafka
  - streaming-data-types
  - CLI11
  - trompeloeil
  - date
  - graylog
- Codebase now requires C++17 to make use of `std::optional`, `std::variant` and `std::filesystem`. `filesystem` is 
used from the `std::experimental` namespace when necessary to support gcc 8 and AppleClang 10. Compile times reduced by
approx 5%, for details of test see PR ([#558](https://github.com/ess-dmsc/kafka-to-nexus/pull/558)).
- The application will no longer fail silently when encountering unit types that it does not recognise when parsing the JSON code for the HDF structure.
- To better work with grafana, you no longer set the service-id. Instead you set the service name which is then used to generate a service id. The service name is set with the ´--service-name´ argument.
- Fixed a bug in the metrics code that prevented more than 10 counters from being pushed to Grafana.
- Flatbuffer verifiers have been re-enabled for command messages.
- Kafka timing settings have been changed to reduce the probability of timeouts.
- Removed a bunch of old (and now unused) Kafka metadata code.
- Multiple improvements to log messages.
- Modified/fixed/cleaned the system tests.
- All writer modules now set a *NX_class* value.
- The *NX_class* value configured for a writer module will always override that of the HDF group *NX_class* value. If configured, a writer module attribute (e.g. *NX_class*) will override both.
- The Kafka topic and flatbuffer source names will now automatically be written as (HDF) attributes when instantiating a new writer module.
- Minor fix to CMake code for working around differences in how file-name leading lower and upper case letters are handled on different systems.
- The writer module configuration names/keys have been unified.
- Better documentation of writer module configuration options.
- The application will now print an error message if there is a configuration that is not used (due to e.g. a typo).
- The error reporting and handling of writer module configurations have overall been greatly improved.
- Added support for writing ADAr schemas.
- A warning/error message will now be produced if an unknown node in the JSON structure is encountered.
- The shape of (multidimensional) static data sets and metadata sets have been unified such they are now in both cases determined from the shape of the actual data in the JSON-code. The `size` field for static data sets will thus be ignored.
- Support for multiple different (non-UTF) text encodings was deemed fragile and incomplete and was thus removed. This means that the use of the `encoding`-field in a JSON structure will result in an error message.
- The JSON structure for defining static and streaming datasets (writer modules in the later case) has been unified and made more logical. Read the corresponding documentation for how these structure should now be defined.
- Support for string array (metadata) HDF% attributes has been removed.
- Command line arguments that sets a duration have been changed into arguments that accepts a unit _e.g._ "200ms".
- Use of SWMR is no longer an option, its mandatory.
- Fixed issues in the SWMR implementation.
- There is now rudimentary for automatically generated meta-data fields in the JSON status update string and HDF5 file. This is currently limited to calculating the min, mean and max in f142 writer module instances and automatically writing the start and end times of a file to that file.
- The application will now attempt to name the processing threads. This should be helpful when running the application under a debugger.
- Replaced many uses of `std::chrono::system_clock_duration` with `std::chrono::system_clock::time_point`. This should make it harder to make some coding mistakes.
- The `spdlog` dependency has been removed.

### Command system re-write

- The command system has been (almost completely) re-written. This change completely breaks backwards compatibility in the command system as there are strict requirements on some of the fields in the "start" and "stop" schemas.
- As a part of this change, the file-writer will now also (if possible and reasonable) respond to a command with a confirmation or failure message. Furthermore, a message will also be sent when the write job has stopped. 
- The code is (currently) the documentation. Instead of implementing your own code for commanding the file-writer, you can use the [file-writer-control](https://github.com/ess-dmsc/file-writer-control) Python library.
- Furthermore, command and status topics have been merged into one command + status topic. The command line argument has thus also been changed to `--command-status-uri`.
- There is now a job pool functionality. The job pool topic is set with the argument `--job-pool-uri`.
- Errors encountered while setting up data streaming or while streaming data from Kafka will now result in the termination of the write job and a message that explains why being posted to the relevant command topic.

## Version 3.0.0, re-write of stream logic

- The core stream loigc has been re-written to be more modularised and properly unit tested.
- `use-hdf-swmr` and `abort-on-uninitialised-stream` are now CLI options rather than configurable per job in the Kafka start command.
- Sending status reports to Kafka is no longer optional.
- Status reports are flatbuffer serialised.
- Using the `graylog-logger` library as a spdlog module is now the default. Set CMake option `USE_GRAYLOG_LOGGER=OFF` to disable.
- CMake "find modules" are now generated by Conan and therefore not required in this repository, disabling Conan (CMake option `CONAN=DISABLE`) and providing your own find modules is still possible.
- The following dependencies have been updated:
  - spdlog-graylog
  - spdlog
  - librdkafka
  - fmt
  - trompeloeil