# *ev42* Detector event

## Example

Example `nexus_structure` to write radiation detector events:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "ev42"
        }
      }
    ]
  }
}
```

## More configuration options

* `nexus.indices.index_every_mb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given megabytes.
* `nexus.indices.index_every_kb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given kilobytes.
* `nexus.chunk.chunk_mb` (int)
  Size of the HDF chunks given in megabytes.
* `nexus.chunk.chunk_kb` (int)
  Size of the HDF chunks given in kilobytes.
* `nexus.buffer.size_mb` (int)
  Small messages can additionally be buffered to reduce HDF writes. This gives
  the buffer size in megabytes.
* `nexus.buffer.size_kb` (int)
  Small messages can additionally be buffered to reduce HDF writes. This gives
  the buffer size in kilobytes.
* `nexus.buffer.packet_max_kb` (int)
  Maximum size of messages to be considered for buffering in kilobytes.
