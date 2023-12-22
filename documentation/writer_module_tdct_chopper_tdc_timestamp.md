# *tdct* Chopper top-dead-center timestamp

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "senv").|
chunk_size|int|No|The HDF5 chunk size in nr of elements. Defaults to `4096`.|
enable_alarm_info|bool|No|Enable or disable EPICS alarm status writing. Defaults to "true".|
enable_epics_con_info|bool|No|Enable or disable EPICS connection status writing. Defaults to "true".|

## Example

Example `nexus_structure` to write chopper TDC timestamps:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "tdct",
        "config": {
          "topic": "the_kafka_topic",
          "source": "the_source_name"
        }
      }
    ]
  }
}
```

