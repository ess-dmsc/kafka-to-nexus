# f142 LogData writer module

## Example

Example `nexus_structure` to write a scalar `double` value:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "f142",
          "type": "double",
          "value_units": "cubits"
        }
      }
    ]
  }
}
```

"value_units" is optional; if it is present the writer module creates a units attribute on the value dataset.

For arrays, we have to specify the `array_size`:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "stream",
        "stream": {
          "topic": "the_kafka_topic",
          "source": "the_source_name",
          "writer_module": "f142",
          "type": "double",
          "array_size": 32
        }
      }
    ]
  }
}
```


## More configuration options

The json paths of these settings are given relative to the object which holds
the `array_size` key in the above examples.

* `nexus.indices.index_every_mb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given megabytes.
* `nexus.indices.index_every_kb` (int)
  Write an index entry (in Nexus terminology: cue entry) every given kilobytes.
* `store_latest_into` _documention missing_
