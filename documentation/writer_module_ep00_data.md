# ep00 LogData writer module

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
