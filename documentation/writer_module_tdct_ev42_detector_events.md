# *ev42* Detector event

## Example

Example `nexus_structure` to write radiation detector events:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "ev42"
        }
      }
    ]
  }
}
```

## More configuration options

*To be completed.*

