# *senv* Sample environment waveform data

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "senv").|
chunk_size|int|No|The HDF5 chunk size in nr of elements. Defaults to 4096.|
enable_epics_con_status|bool|No|Enable or diable EPICS connection status writing. Defaults to "true".|

## Example

Example `nexus_structure` to write sample environment wave forms:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "senv",
        "config": {
          "topic": "the_kafka_topic",
          "source": "the_source_name"
        }
      }
    ]
  }
}
```


