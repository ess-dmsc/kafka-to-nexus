# Status messages

The file-writer will publish regular status messages to the Kafka topic defined for status messages.
The time between message is defined by the `status-master-interval` configuration parameter.

Status messages are serialised as FlatBuffers using schema 
[x5f2_status.fbs](https://github.com/ess-dmsc/streaming-data-types/blob/master/schemas/x5f2_status.fbs).
This schema is used by several applications but also contains a field named `status_json` in which to put
application specific details, serialised as JSON. The common fields of the schema are documented in the
[schema file](https://github.com/ess-dmsc/streaming-data-types/blob/master/schemas/x5f2_status.fbs),
so here we will only document what the Filewriter populates the `status_json` with.

If all is fine and the file-writer is idle then the JSON would be something like:

```json
{
    "file_being_written": "",
    "job_id": "",
    "start_time": 0,
    "stop_time": 0
}
```

If a file is being written then it would be something like this:

```json
{
    "file_being_written": "some_file.nxs",
    "job_id": "1234",
    "start_time": 1580460273162,
    "stop_time":  9223372036854
}
```

These messages always contain the name of the file being written, the associated job ID and time writing started (note: 
could be in the past). The stop time will be very large (9223372036854) if no stop time was supplied when the writing 
was started; however, status messages published after a stop command is sent will contain the requested stop time. 
