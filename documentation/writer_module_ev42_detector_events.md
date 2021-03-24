# *ev42* Detector event

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "ev42").|
cue_interval|int|No|The interval (in nr of events) at which indices for searching the data should be created. Defaults to _never_.|
chunk_size|int|No|The HDF5 chunk size in nr of elements. Defaults to 1M.|
adc_pulse_debug|bool|No|Should ADC debug data be written (if present)?. Defaults to `false`.|


## Example

Example `nexus_structure` to write radiation detector events:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "ev42",
        "config": {
          "topic": "the_kafka_topic",
          "source": "the_source_name"
        }
      }
    ]
  }
}
```
