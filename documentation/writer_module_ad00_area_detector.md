# *al00* Alarm information

## Stream configuration fields

|Name|Type|Required| Description                                                                                                        |
---|---|---|--------------------------------------------------------------------------------------------------------------------|
topic|string|Yes| The kafka topic to listen to for data.                                                                             |
source|string|Yes| The source (name) of the data to be written.                                                                       |
writer_module|string|Yes| The identifier of this writer module (i.e. "ad00").                                                                |
cue_interval|int|No| The interval (in nr of events) at which indices for searching the data should be created. Defaults to 100 million. |
chunk_size|int|No| The HDF5 chunk size in nr of elements. Defaults to 1M.                                                             |


### Example

Example `nexus_structure`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "ad00",
        "config": {
          "source": "the_source_name",
          "topic": "the_topic_name",
          "array_size": "$AREADET$",
          "dtype": "int16"
        }
      }
    ]
  }
}
```
Typically, the `$AREADET$` placeholder is replaced at runtime by NICOS. 
Alternatively, it can contain an array of values corresponding to width and height.
