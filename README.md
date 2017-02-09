# Kafka to Nexus file writing

Documents:

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
* (eventually) multiple Source per streamer
* acts according the function ```f( msg->payload() )``` specified in ```write(f)```
* has to be able to search back in the kafka queue for the first message. Some
  slow sources can be (much) older than the DAQ starts and updated not
  frequently, we must be able to retrieve the
  informations. ```search_backward(f)``` implements some algorithm that uses the
  function ```f``` to find the older message with good timestamp. Different
  sources can have data in different point of the queue: Source has to discard
  invalid (according to timestamp) data
* if the broker is not valid (_e.g._ a change of IP address) it should notify
  the FileMaster, retrieve the new configuration and reconnect

In the current implementation the Streamer does not "contains" the corresponding
sources. They are part of a different container in the StreamMaster. A different
solution would be make the sources to be contained in the corresponding streamer.

## DemuxTopic
Mapped 1:1 with topics (and Streamers) drives the message to the correct Source.

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

## Running tests


Usage:
```bash
   bin/streamer_test --kafka_broker=<broker>:<port>  --kafka_topic="<topic name>"
   bin/streammaster_test --kafka_broker=<broker>:<port>"
```
Tests are implemented using the gtest suite. They support all the command
line option provided by gtest.
