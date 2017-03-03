# Kafka to Nexus file writing

## Requirements

- rapidjson
- libfmt
- `streaming-data-types` repository (clone e.g. in the same directory as `kafka-to-nexus`)
- librdkafka
- pcre2 (`yum install pcre2 pcre2-devel` or `brew install pcre2`)
  (Needed because we support GCC < 4.9 where std regex is incomplete)


## Documents:

[Nexus-for-ESS](https://ess-ics.atlassian.net/wiki/display/DMSC/NeXus+for+ESS)

[file-writer-2016-10-28](https://ess-ics.atlassian.net/wiki/download/attachments/48202445/BrightNeXus.pdf?version=1&modificationDate=1477659873237&cacheVersion=1&api=v2)

The overall design of the file writer is described in
[NeXusFileWriterDesign](NeXusFileWriterDesign.md) (Mark). It is used as a sort
of "guideline".

## Graph of dependencies and data flow

 Very early draft so far:

![Flow](flow.svg)

A pictorial representation of the implementation is ![File Writer overall design](docs/FileWriter.jpg)

## Streamer

According to the design the Streamer connects to Kafka (other
sources to be implemented) and consumes a message in the specified topic. Some features:
* one Streamer per topic
* multiple Source per streamer
* has to be able to search back in the kafka queue for the first message. Some
  slow sources can be (much) older than the DAQ starts and updated not
  frequently, we must be able to retrieve the
  informations. ```search_backward(f)``` implements some algorithm that uses the
  function ```f``` to find the older message with good timestamp. Different
  sources can have data in different point of the queue: Source has to discard
  invalid (according to timestamp) data
* if the broker is not valid (_e.g._ a change of IP address) it should notify
  the FileMaster, retrieve the new configuration and reconnect


## DemuxTopic
Mapped 1:1 with topics (and Streamers) drives the message to the correct Source. Derived from classes MessageProcessor and TimeDifferenceFromMessage. The former provides an interface for processing new messages (usually write on disk), the latter the interface process old messaged with the aim of find the first message sent after ECP ```start ```message.
The two corresponding methods are
* process_message
* time_difference_from_message
Both receive the message payload and size. Return values are ProcessMessageResult and TimeDifferenceFromMessage_DT.

## Source

## StreamMaster
The StreamMaster receives the array of DemuxTopic from FileWriterCommand and
instantiates the Streamer array according to the topics. Eventually retrieves
the list of brokers from Kafka.

* ``the timestamp_list`` is useful for look for initial status of sources in the
  Kafka queue. It has to be used in combination with Streamer
  ``search_backward``. It maps the topic with the vector of pairs
  source-timestamp difference _w.r.t._ the offset of DAQ start provided by ECP
* after instantiation **searches** in the Kafka queue the _OFFSET_ of the oldest
  "good" value (due to synchronisation issues, slow sensors, etc)
* for each topic iterates over the Sources . Listen on each Source until
  - the message queue is empty
  - Streammaster::duration milliseconds have been elapsed
* when receives a **termination** command from Master closes all the streamers


## Flatbuffer Schema Plugins

The actual parsing of the different FlatBuffer schemata is handled by plugins
which register themself via the `SchemaRegistry`.
See for example `kafka-to-nexus/src/schema_f141.cxx:331`.
Support for new schemas can be added in the same way.


## Running tests

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

More tests involing the network:
```
tests/streamer_test --kafka_broker=<broker>:<port>  --kafka_topic="<topic name>"
tests/streammaster_test --kafka_broker=<broker>:<port>"
```
Tests are implemented using the gtest suite. They support all the command
line option provided by gtest.
