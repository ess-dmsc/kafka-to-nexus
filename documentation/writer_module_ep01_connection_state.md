# *ep01* EPICS connection status

## Stream configuration fields
|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "ep01").|
chunk_size|int|No|The HDF5 chunk size in nr of elements. Defaults to 1024.|

## Example

Example `nexus_structure`:

```json
{
"nexus_structure": {
   "children": [
        {
            "type": "group",
            "name": "EpicsConnectionStatus",
            "children": [
              {
                "module": "ep01",
                "config": {
                  "topic": "ExampleTopic",
                  "source": "SIMPLE:DOUBLE"
                }
              }
            ]
        }
    ]
  }
}
```
