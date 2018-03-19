# Groups

Groups are analogous to directories in a filesystem. They can have datasets,
streams or other groups in their array of `children`. They can also have
[attributes](attributes.md).

Possible fields are:
- `"type": "group"` (required)
- `"name": "<GROUP NAME>"` (required)
- `"children": []` - array may contain `dataset`, `stream`, `group`.
- `"attributes":` - object or array of [attributes](attributes.md).

Example:
```json

{
  "type": "group",
  "name": "instrument",
  "attributes": {
    "NX_class": "NXinstrument"
  },
  "children": [
    {
      "type": "dataset",
      "values": 42
    }
  ]
}
```
