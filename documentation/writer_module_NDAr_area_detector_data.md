# *NDAr* EPICS area detector data

__Note: This writer module is deprecated and has been replaced by the *ADAr* writer module.__

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "senv").|
array_size|list of ints|No|The shape of the data to be written. Defaults to [1, 1]|
chunk_size|list of ints|No|The shape of the chunk. Should have the same number of elements as array_size. Defaults to 1M.|
type _or_ dtype|string|No|The type of the data to be writting. If the received data is not of this type, the module will try to convert it. Defaults to _double_.|
cue_interval|int|No|The interval (in nr of received messages) at which indices for searching the data should be created. Defaults to 1000.|

## Example

Example `nexus_structure` to write sample environment wave forms:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "NDAr",
        "config": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "array_size": [256,256],
          "chunk_size": [2048,256],
          "dtype": "uint64",
          "cue_interval": 5000
        }
      }
    ]
  }
}
```


