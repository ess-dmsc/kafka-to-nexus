# EventHistogram writer module

Schema `hs00_event_histogram.fbs` defines the `EventHistogram` with flatbuffer
schema id `hs00`.

It is used to exchange multidimensional histogrammed data, errors and the
description of their axes.

We can write the `EventHistogram` stream to HDF with a child in the
`nexus_structure` like:

```json
{
  "type": "stream",
  "stream": {
    "topic": "topic.with.multiple.sources",
    "source": "some_histogram_producer",
    "writer_module": "hs00",
    "data_type": "uint64",
    "error_type": "double",
    "edge_type": "double",
    "shape": [
      {
        "size": 4,
        "label": "Position",
        "unit": "mm",
        "edges": [2, 3, 4, 5, 6],
        "dataset_name": "x_detector"
      },
      {
        "size": 6,
        "label": "Position",
        "unit": "mm",
        "edges": [-3, -2, -1, 0, 1, 2, 3],
        "dataset_name": "y_detector"
      },
      {
        "size": 3,
        "label": "Time",
        "unit": "ns",
        "edges": [0, 2, 4, 6],
        "dataset_name": "time_binning"
      }
    ]
  }
}
```

In this command, several data types are defined:

- `data_type`: The type of the array in the flatbuffer member
  `EventHistogram.data.value`
- `error_type`: The type of the array in the flatbuffer member
  `EventHistogram.errors`
- `edge_type`: The type of the array in the flatbuffer member
  `EventHistogram.dim_metadata.bin_boundaries`

All these three types must be one of `uint32`, `uint64`, `float` or `double`.

A single histogram may be represented as multiple `EventHistogram` messages on
the Kafka topic.  All parts must have the same `EventHistogram.timestamp`.  The
individual parts must not overlap.
