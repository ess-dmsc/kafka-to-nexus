# f142 LogData writer module

## Example

Example `nexus_structure` to write a scalar `double` value:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "f142",
          "type": "double"
        }
      }
    ]
  }
}
```

For `double` arrays, we have to specify the `array_size`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "f142",
          "type": "double",
          "array_size": 32
        }
      }
    ]
  }
}
```


## More configuration options

The json paths of these settings are given relative to the object which holds
the `array_size` key in the above examples.

* `nexus.indices.index_every_mb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given megabytes.
* `nexus.chunk.chunk_mb` (int)
  Size of the HDF chunks given in megabytes.
* `nexus.buffer.size_kb` (int)
  Small messages can additionally be buffered to reduce HDF writes. This gives
  the buffer size in kilobytes.
* `nexus.buffer.packet_max_kb` (int)
  Maximum size of messages to be considered for buffering in kilobytes.
