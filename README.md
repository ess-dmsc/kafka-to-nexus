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
* acts according the function ```f( msg->payload() )``` specified in ```recv(f)```
* has to be able to search back in the kafka queue for the first message. Some
  slow sources can be (much) older than the DAQ starts and updated not
  frequently, we must be able to retrieve the
  informations. ```search_backward(f)``` implements some algorithm that uses the
  function ```f``` to find the older message with good timestamp. Different
  sources can have data in different point of the queue: Source has to discard
  invalid (according to timestamp) data
* if the broker is not valid (_e.g._ a change of IP address) it should notify
  the FileMaster, retrieve the new configuration and reconnect

## Source

## StreamMaster


## Running tests

Tests currently make use of the KakaMock implementation. In principle there is no need for a real
kafka borker. Nevertheless, it would be nice to be able to run with the mock
version and the real broker.

Usage:
```bash
   bin/streamer_test --kafka_broker=<broker>:<port>  --kafka_topic="<topic name>"
   ```
Tests are implemented using the gtest suite. They support all the command
line option provided by gtest.
