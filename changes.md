# Changes

## Next version

- Adding (tcp based) service api to query a filewriter for its status
- Fix: Cannot import extra modules due to mismatch in ep00, ep01 and al00 registered names

## Version 6.0.0

- Breaking: Run start messages (pl72 schema) now require a `start_time` field.
- Breaking: Ignore Kafka IP addresses sent in `StartJob` messages, experiment
  data is now fetched from the broker configured in `job-pool-uri`.
- Commands received without an explicit kafka-to-nexus `service_id` are no
  longer skipped. The command is processed by all workers and accepted by the
  worker with a matching `job_id`.
- Fix: The case when messages are not received from a specific Kafka topic does not
  make the file writer unsubscribe from the topic anymore. Instead a warning is
  provided in the file writer log.
- Fix: Stop commands sent immediately after a start command were not always processed.
- Fix: Redact Kafka SASL password in logs.
- Adding _f144_, _al00_ and _ep01_ writer modules. For more information on the schemas mentioned,
  see ([schema definitions here](https://github.com/ess-dmsc/streaming-data-types)).
- Adding _se00_ writer module (see [schema definitions here](https://github.com/ess-dmsc/streaming-data-types)).
- Adding _ev44_ writer module (see [schema definitions here](https://github.com/ess-dmsc/streaming-data-types)).
- Ignore deprecated warnings on macOS (can be removed when https://github.com/chriskohlhoff/asio/issues/1183 is addressed.
- Enable idempotence setting in the Kafka producer.
- Updated librdkakfa Conan package version to 2.0.2



## Version 5.2.0: Kafka improvements and other fixes

- Increased kafka message buffer sizes and added integration tests for this.
- Improved help text formatting.
- Added code for running Kafka tests. This code is disabled by default.
- It is now possible to set the Kafka poll timeout from the command line. This option should rarely (if ever) be used.
- The following dependencies have been updated:
  - graylog-logger ([#650](https://github.com/ess-dmsc/kafka-to-nexus/pull/650))
- Enabled SSL and SASL in librdkafka to support Kafka authentication.
- Fix to make all Kafka connections honour the provided librdkafka parameters.
- Silencing x5f2 schema message and file writer not currently writing status message.


## Version 5.1.0: Attributes and dependencies

- Renamed system tests to integration tests.
- Added system test to verify proper handling of bad "start writing" messages.
- Writer module attributes will now be ignored. If you want to set attributes of a parent group, do so directly.
- Prioritisation has changed such that the *NX_class* of a parent group to a file-writer module will only be set if no such attribute is already set.
- All the dependencies have been updated to their latest (conan) version as of 2022-05-23. This has required some changes and bug fixes in the application.


## Version 5.0.0: Usage and opinion

- The filewriter will now only run in the job pool mode. This means that the user is required to supply a job pool topic in the filewriter .ini configuration flag using the --job-pool-uri option.
- Improved log messages and thread names. Done to aid debugging.
- Changed _fetch.message.max.bytes_ Kafka variable back to its default value as it was causing timeouts.
- Fix of bug in setting up the console logger interface.
- Warning (log) messages will now be produced if the first message from a source has a data type different than that configured for the current writer module instance. This has been implemented for the `f142`, `senv` and `ADAr` modules.
- The Grafana metrics prefix now has the form "kafka-to-nexus.*hostname*.*service_name*" if the service name is set. If not, it has the form "kafka-to-nexus.*hostname*.*service_id*".
- Fix ordering of elements in static data.
- Added functionality for automatically instantiating extra writer modules.
- The writer modules _f142_, _senv_ and _tdct_ will now automatically also instantiate _ep00_ writer modules at the same location. Disable this by setting the `enable_epics_con_status` config option to `false`.
- The `--abort-on-uninitialised-stream` command line option has been removed. An error in the JSON code for initialising a stream will now always cause an error that will stop further initialisation.
- Added documentation.
- Potentially fixed a bug where the file-writer gets into a bad state.
- Removed automatic "start_time" and "end_time" metadata fields.
- Simplified system test code.


## Version 4.1.0: Quality of life

- Each ev42 writer module instance will now publish the number of events written to file.
- It is no longer possible to set a stop time if the previously set stop time has passed. Also added unit tests for this feature.
- The application will now give you an approximate size of the file it is writing to, rounded up to the nearest 10 MB, in the status messages that it produces.
- The f142 cue index functionality was unintentianlly disabled but has now been restored to working order. Unit tests were added to prevent future such regression in the code.
- Creating links should now work properly. The configuration of links has been changed
    and is now closer to how it is configured for datasets and streams, i.e. as a module configuration.
    Unit tests and system test added for link creation cases.
    ([#607](https://github.com/ess-dmsc/kafka-to-nexus/pull/607))
- Fixed a bug where the file-writer will not abandon an alternative command topic if it fails to start a file-writing job.
- The f142 value statistics written to file is now done so according to the NeXus format.
- Added the HDF5/NeXus file structure to the "writing finished"-message and a system test to check this.


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
