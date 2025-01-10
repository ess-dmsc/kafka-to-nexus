# mdat writer module

## Stream configuration fields

This module is different to other writer modules in that it doesn't use Kafka. Instead metadata data values are set via
code.
It isn't a general metadata writer, there are only a discrete set of named values it will work with. Other values are 
ignored.

Currently, it only supports start and stop times.

## Example
Example `nexus_structure`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "entry",
        "children": [
          {
            "module": "mdat",
            "config": {
              "items": ["start_time", "end_time"]
            }
          }
        ]
      }
    ]
  }
}
```
