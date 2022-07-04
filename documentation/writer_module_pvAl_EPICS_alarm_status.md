# pvAl EPICS connection status writer module

__Note: This writer module will by default be initialised by several other (EPICS data) writer modules. It is therefore unlikely that you will ever need to instantiate this module manually.__

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "pvAl").|

## Example

Example `nexus_structure` to write status messages from `ExampleTopic`:

```json
{
"nexus_structure": {
   "children": [
        {
            "type": "group",
            "name": "EpicsAlarmStatus",
            "children": [
              {
                "module": "pvAl",
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
