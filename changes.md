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
- All writer modules now set a *NX_class* value.
- The *NX_class* value configured for a writer module will always override that of the HDF group *NX_class* value. If configured, a writer module attribute (e.g. *NX_class*) will override both.
- The Kafka topic and flatbuffer source names will now automatically be written as (HDF) attributes when instantiating a new writer module.
- Minor fix to CMake code for working around differences in how file-name leading lower and upper case letters are handled on different systems.
- The writer module configuration names/keys have been unified.
- Better documentation of writer module configuration options.
- The application will now print an error message if there is a configuration that is not used (due to e.g. a typo).
- The error reporting and handling of writer module configurations have overall been greatly improved.
