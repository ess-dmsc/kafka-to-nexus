# Status messages and command responses

## Status messages
The file-writer will publish regular status messages to the Kafka topic defined for status messages.
The time between message is defined by the `status-master-interval` configuration parameter.

The message format is JSON and the whitespace formatting may vary from what is shown here.

If all is fine and the file-writer is idle then the message would be something like:

```json
{
    "file_being_written": "",
    "job_id": "",
    "start_time": 0,
    "stop_time": 0,
    "update_interval": 2000
}
```

The `update_interval` is used to inform clients how often status message can be expected, so they can
judge whether communication is lost.

If a file is being written then the status message would be something like this:

```json
{
    "file_being_written": "some_file.nxs",
    "job_id": "1234",
    "start_time": 1580460273162,
    "stop_time": 0,
    "update_interval": 2000
}
```

These messages always contain the name of the file being written, the associated job ID and time writing started (note: 
could be in the past). The stop time will be 0 if no stop time was supplied when the writing was started; however, it will
be populated when a stop command is sent. 
