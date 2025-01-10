# *al00* Alarm information

## Stream configuration fields

|Name|Type|Required| Description                                                                                                        |
---|---|---|--------------------------------------------------------------------------------------------------------------------|
topic|string|Yes| The kafka topic to listen to for data.                                                                             |
source|string|Yes| The source (name) of the data to be written.                                                                       |
writer_module|string|Yes| The identifier of this writer module (i.e. "al00").                                                                |
cue_interval|int|No| The interval (in nr of events) at which indices for searching the data should be created. Defaults to 100 million. |
chunk_size|int|No| The HDF5 chunk size in nr of elements. Defaults to 1M.                                                             |


### Example

Example `nexus_structure`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "al00",
        "config": {
          "source": "the_source_name",
          "topic": "the_kafka_topic",
          "dtype": "double"
        }
      }
    ]
  }
}
```
