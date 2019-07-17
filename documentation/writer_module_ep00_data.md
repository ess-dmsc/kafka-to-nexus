# ep00 LogData writer module

## Example

Example `nexus_structure` to write status messages from `ExampleTopic`:

```json
{
"nexus_structure": {
   "children": [
        {
            "type": "group",
            "name": "EpicsConnectionStatus",
            "children": [
              {
                "type": "stream",
                "stream": {
                  "topic": "ExampleTopic",
                  "source": "SIMPLE:DOUBLE",
                  "writer_module": "ep00"
                }
              }
            ]
        }
    ]
  }
}
```



## More configuration options

The json paths of these settings are given relative to the object which holds
the `array_size` key in the above examples.

* `nexus.chunk.chunk_mb`(`_kb`) (int)
  Size of the HDF chunks given in megabytes (kilobytes).
* `nexus.buffer.size_mb`(`_kb`) (int)
  Small messages can additionally be buffered to reduce HDF writes. This gives
  the buffer size in megabytes (kilobytes).
* `nexus.buffer.packet_max_kb` (int)
  Maximum size of messages to be considered for buffering in kilobytes.
