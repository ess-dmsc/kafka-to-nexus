# Status messages and command responses

## Status messages
The file-writer will publish regular status messages to the Kafka topic defined for status messages.
The time between message is defined by the `status-master-interval` configuration parameter.

The message format is JSON and the whitespace formatting may vary from what is shown here.

If all is fine and the file-writer is idle then the message would be something like:

```json
{
    "files":{},
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:59001",
    "type":"filewriter_status_master"
}
```

If the a file is being written then the status message would be something like this:

```json
{
    "files":{
        "8bacf956-02a3-11e9-af16-64006a47d649":{
            "filename":"/data_files/test.nxs",
            "topics":{
                "TEST_sampleEnv":{
                    "error_message_too_small":0,
                    "error_no_flatbuffer_reader":0,
                    "error_no_source_instance":0,
                    "messages_processed":0
                    }
                }
            }
        },
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:59001",
    "type":"filewriter_status_master"
}
```

This gives the job-id (in this case 8bacf956...) and the topics being written. For each topic it provides the number of 
messages processed (i.e. values written) and some error statistics.

The status message is usually followed by a message giving a more general status overview; it will look something like:

```json
{
    "job_id": "8bacf956-02a3-11e9-af16-64006a47d649",
    "next_message_eta_ms": 2000,
    "stream_master": {
        "Mbytes": 0.0,
        "errors": 0,
        "messages": 0,
        "runtime": 28042,
        "state": "Running"
    },
    "streamer": {
        "TEST_sampleEnv": {
            "rates": {
                "Mbytes": 0.0,
                "errors": 0,
                "message_size": {
                    "average": 0.0,
                    "standard_deviation": 0.0
                },
                "messages": 0
            }
        }
    },
    "timestamp": 437227568,
    "type": "stream_master_status"
}
```
It provides some useful metrics about the amount of data being processed.

## Command responses

Upon receiving a command from a client (NICOS, cmd-line etc.) the file-writer will publish a response of the status
topic.

Assuming the file-writer is happy with the command then it will respond with a message like:

```json
{
    "code":"START",
    "job_id":"8bacf956-02a3-11e9-af16-64006a47d649",
    "message":"Start job",
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:16055",
    "timestamp":1551360732952,
    "type":"filewriter_event"
}
```
Then it will continue sending status messages as described above.

If the file-writer is unhappy with the command then it will respond with three messages.
A START message to acknowledge the request, like so:

```json
{
    "code":"START",
    "job_id":"8bacf956-02a3-11e9-af16-64006a47d649",
    "message":"Start job",
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:10307",
    "timestamp":1551344628798,
    "type":"filewriter_event"
}
```

A CLOSE message to indicate it has stopped trying to write, for example:

```json
{
    "code":"CLOSE",
    "job_id":"8bacf956-02a3-11e9-af16-64006a47d649",
    "message":"File closed",
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:10307",
    "timestamp":1551344628813,
    "type":"filewriter_event"
}
```

And either an ERROR message or a FAIL message describing the reason for not writing.
 
An example ERROR message might be:

```json
{
    "code":"ERROR",
    "job_id":"8bacf956-02a3-11e9-af16-64006a47d649",
    "message":"Configuration Error",
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:10307",
    "timestamp":1551344628813,
    "type":"filewriter_event"
}
```

In this case, the message indicates that there was an issue with the configuration supplied in the command.

A example FAIL message might be:

```json
{
    "code":"FAIL",
    "job_id":"8bacf956-02a3-11e9-af16-64006a47d649",
    "message":"Unexpected std::exception while handling command:{\n  \"cmd\": \"FileWriter_new\",\n  \"broker\": \"127.0.0.1:9092\",\n  \"job_id\": \"8bacf956-02a3-11e9-af16-64006a47d649\",\n  \"file_attributes\": {\n    \"file_name\": \"test.nxs\"\n  },\n  \"nexus_structure\": {\n      \"children\": [\n          {\n            \"type\": \"group\",\n            \"name\": \"my_test_group\",\n            \"children\": [\n              {\n                \"type\": \"stream\",\n                \"stream\": {\n                  \"dtype\": \"double\",\n                  \"writer_module\": \"f142\",\n                  \"source\": \"my_test_pv\",\n                  \"topic\": \"LOQ_sampleEnv\"\n                }\n              }\n            ],\n            \"attributes\": [\n              {\n                \"name\": \"units\",\n                \"values\": \"ms\"\n              }\n            ]\n          }\n        ]\n      }\n}\n\nError in CommandHandler::tryToHandle\n  Failed to initializeHDF: can not initialize hdf file /data_files/test.nxs\n    can not initialize hdf file /Users/mattclarke/Code/Repos/DMSC/kafka-to-nexus/cmake-build-debug/bin/test.nxs\n      The file \"/Users/mattclarke/Code/Repos/DMSC/kafka-to-nexus/cmake-build-debug/bin/test.nxs\" exists already.",
    "service_id":"kafka-to-nexus--host:SERVERNAME--pid:10307",
    "timestamp":1551344628813,
    "type":"filewriter_event"
}
```

The message is a bit verbose but it can be seen that the issue in this case is that a file with the requested name
already exists.

A FAIL message indicates a general problem related to the command sent to the file-writer, for example: a file already 
existing or malformed JSON.
An ERROR message represents a more specific problem related to the act of file-writing, for example: a Kafka topic not 
existing or not being able to reopen the NeXus file.

Note: these messages can in theory arrive in any order and, also, be separated by the regular status messages.
