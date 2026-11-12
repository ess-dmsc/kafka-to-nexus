# _vs00_ Logdata

Used for string data, e.g. explicit positions of devices with values like 'in beam', 'out of beam'.

## Stream configuration fields

| Name          | Type   | Required | Description                                         |
| ------------- | ------ | -------- | --------------------------------------------------- |
| topic         | string | Yes      | The kafka topic to listen to for data.              |
| source        | string | Yes      | The source (name) of the data to be written.        |
| writer_module | string | Yes      | The identifier of this writer module (i.e. "vs00"). |

### Example

Example `nexus_structure`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "vs00",
        "config": {
          "source": "the_source_name",
          "topic": "the_kafka_topic",
          "dtype": "string"
        }
      }
    ]
  }
}
```
