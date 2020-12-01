# ep00 LogData writer module

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "ep00").|

## Example

Example `nexus_structure` to write status messages from `ExampleTopic`:

```json
{
"nexus_structure": {
   "children": [
        {
            "type": "group",
            "name": "EpicsConnectionStatus",
            "children": [
              {
                "type": "stream",
                "stream": {
                  "topic": "ExampleTopic",
                  "source": "SIMPLE:DOUBLE",
                  "writer_module": "ep00"
                }
              }
            ]
        }
    ]
  }
}
```
