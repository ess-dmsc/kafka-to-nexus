# Changes

*Add to release notes and clear this list when creating a new release*

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
- Codebase now requires C++17 to make use of `std::optional`, `std::variant` and `std::filesystem`. `filesystem` is 
used from the `std::experimental` namespace when necessary to support gcc 8 and AppleClang 10. Compile times reduced by
approx 5%, for details of test see PR ([#558](https://github.com/ess-dmsc/kafka-to-nexus/pull/558)).
- The application will no longer fail silently when encountering unit types that it does not recognise when parsing the JSON code for the HDF structure.
- The command system has been re-done. The code is (currently) the documentation but it is stronly suggested that instead of reading that, you use the (_file-writer-control_)[https://github.com/ess-dmsc/file-writer-control] Python library to control the file-writer.
- Some old and now out of date documentation on controlling the file-writer has been deleted.
- Command and status topics have now been merged into one command + status topic. The command line argument has thus also been changed to `--command-status-uri`.
- There is now a job pool functionality. The job pool topic is set with the argument `--job-pool-uri`.
- To better work with grafana, you no longer set the service-id. Instead you set the service name which is then used to generate a service id. The service name is set with the ´--service-name´ argument.
- Fixed a bug in the metrics code that prevented more than 10 counters from being pushed to Grafana.
- Flatbuffer verifiers have been re-enabled for command messages.
- Kafka timing settings have been changed to reduce the probability of timeouts.
- Removed a bunch of old (and now unused) Kafka metadata code.
- Improved log messages slightly.
- Modified/fixed/cleaned the system test.
- All writer modules now set a *NX_class* value.
- The *NX_class* value configured for a writer module will always override that of the HDF group *NX_class* value. If configured, a writer module attribute (e.g. *NX_class*) will override both.
- The Kafka topic and flatbuffer source names will now automatically be written as (HDF) attributes when instantiating a new writer module.
- Minor fix to CMake code for working around differences in how file-name leading lower and upper case letters are handled on different systems.
- The writer module configuration names/keys have been unified.
- Better documentation of writer module configuration options.
- The application will now print an error message if there is a configuration that is not used (due to e.g. a typo).
- The error reporting and handling of writer module configurations have overall been greatly improved.
