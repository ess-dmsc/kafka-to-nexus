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

* `adc_pulse_debug` (bool)
  If present and set to `true`, indicates the writer should create a group called 
  "adc_pulse_debug" in the event data group and record any ADC pulse debug data it 
  receives in the event messages. ADC pulse debug data uses the `dtdb` schema and is 
  included as an optional field in the `ev42` schema.
* `nexus.indices.index_every_mb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given megabytes.
* `nexus.indices.index_every_kb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given kilobytes.
* `nexus.chunk.chunk_mb` (int)
  Size of the HDF chunks given in megabytes.
* `nexus.chunk.chunk_kb` (int)
  Size of the HDF chunks given in kilobytes.
