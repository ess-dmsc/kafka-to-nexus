# *tdct* Chopper top-dead-center timestamp

## Example

Example `nexus_structure` to write chopper TDC timestamps:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "tdct"
        }
      }
    ]
  }
}
```

## More configuration options

There are no options for this module.

