# f142 LogData writer module

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "f142").|
cue_interval|int|No|The interval (in nr of events) at which indices for searching the data should be created. Defaults to _never_.|
chunk_size|int|No|The HDF5 chunk size in nr of rows. Defaults to 64x1024.|
array_size|int|No|The size of the array in nr of columns. That is: the number of value elements per flatbuffer message. Defaults to 1. |
type _or_ dtype|string|No|The data type of incoming data. Defaults to `double`. The writer module will try to convert the data to the given (or default) data type.|
value_units _or_ unit|string|No|Sets the attribute "units" of the `value` data set. Will not be set if left as an empty string.|

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
          "type": "double",
          "value_units": "cubits"
        }
      }
    ]
  }
}
```

"value_units" is optional; if it is present the writer module creates a units attribute on the value dataset.

For arrays, we have to specify the `array_size`:

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
