# *senv* Sample environment waveform data

## Example

Example `nexus_structure` to write sample environment wave forms:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "senv"
        }
      }
    ]
  }
}
```

## More configuration options

There are no options for this module.

