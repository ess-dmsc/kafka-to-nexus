s DataArray writer module

## Stream configuration fields

### da00_DataArray table
| Field Name   | Type                | Description                                  | Default        |
|--------------|---------------------|----------------------------------------------|----------------|
| topic        | string              | The kafka topic to listen to for data.       |                |
| source       | string              | The source (name) of the data to be written. |                |
| title        | string              | The title of the HDF5 group.                 |                |
| cue_interval | int                 | The number of messages between cues.         | 1000           |
| chunk_size   | int                 | The HDF5 chunk size.                         | 2<sup>20</sup> |
| variables    | list[da00_Variable] | The variable datasets to writer              | []             |
| constants    | list[da00_Variable] | The constant datasets to write.              | []             |
| attributes   | list[da00_Variable] | The group attributes to write.               | []             |


### Notes on `variables`, `constants`, `attributes`
Most fields in the buffered messages (received in a single write-request) must be constant.
The `data` field in `da00_Variable` is the only exception, and may contain different values in each message if 
the object is listed in the `variables` `da00_DataArray` field.

Since the contained values may change, `variables` are written to time-series datasets in the HDF5 file.
In contrast, `constants` must contain the same value in every message, so are written to scalar datasets in the HDF5 file.
Finally, `attributes` are special constant datasets that are written to the HDF5 group attributes;
they should be small and contain metadata about the group.


### da00_Variable table
| Field Name         | Type         | Description                                   | Mutable | Missing               | `null`                    |
|--------------------|--------------|-----------------------------------------------|---------|-----------------------|---------------------------|
| name               | string       | The name of the dataset or attribute.         | No      | Error                 | Error                     |  
| unit               | string       | The unit along this dimension.                | No      | Omitted               | Fixed-size, first message | 
| label              | string       | The label of the dimension.                   | No      | Omitted               | Fixed-size, first message |
| source             | string       | The source (name) of the data to be written.  | No      | Omitted               | Fixed-size, first message |
| data_type          | string       | The element type of the written data.         | No      | `type(data)` or Error | Error                     |
| axes               | list[string] | The names of the dimensions of the Variable   | No      | Omitted               | Fixed-size, first message | 
| shape              | list[int]    | The size of the histogram in each dimension   | No      | Default               | Error                     | 
| data (variables)   | *            | Ignored for variable datasets (unless `null`) | Yes     | Ignored               | Error                     |
| data (constants)   | *            | The constant values of the Variable.          | No      | First message         | First message             |
| data (attributes)  | *            | The constant values of the Variable.          | No      | First message         | First message             |

The `data` field is special, in that it can _either_ be a vector of values
or a object specification of the form `{"first": number, "last": number, "size": number}`,
in which case it is interpreted as a range of values from `first` to `last` in steps of (`last` - `first`) / (`step` - 1).
For `variables` listed objects, the `data` field is ignored since it is _always_ set by the received message.
For `constants` and `attributes` the `data` field can be omitted if the `data_type` and `shape` fields are set;
similarly, `data_type` and/or `shape` can be omitted if `data` is set, in which case the type and shape are inferred from the data.

Since the `name`, `data_type`, `shape` and whether the object is a `variable` or `constant` are necessary to define
an appropriate HDF5 dataset, these fields or information from which they can be inferred are required in the
configuration for any object that is not listed in the `variables` or `constants` field of the `da00_DataArray`.

## Examples

Schema `da00_dataarray_generated.h` defines the `da00_DataArray` with flatbuffer schema id `da00`.

It is used to exchange multidimensional data, and optionally errors and axis descriptions.

We can write the `da00_DataArray` stream to HDF with a child in the `nexus_structure` like:

```json
{
  "module": "da00",
  "config": {
    "topic": "topic.with.multiple.sources",
    "source": "some_dataarray_producer",
    "variables": [
      {
        "name": "signal",
        "data_type": "uint64",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "signal_errors",
        "data_type": "float64",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "auxiliary",
        "data_type": "float32",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      }
    ],
    "constants": [
      {
        "name": "x",
        "data_type": "float32",
        "axes": ["x"],
        "shape": [1000]
      },
      {
        "name": "y",
        "data_type": "float32",
        "axes": ["y"],
        "shape": [1000]
      }
    ]
  }
}
```
In the above example, the `data_type` and `shape` fields are required in each `variables` and `constants` object
since they do not include `data`. The `axes` field could be read from the received messages, but is included
in the configuration for clarity.

Finally, a somewhat realistic configuration would specify the expected contents of the buffered Variable objects
without relying on information from received messages:

```json
{
  "module": "da00",
  "config": {
    "topic": "topic.with.multiple.sources",
    "source": "some_dataarray_producer",
    "attributes": [
      {
        "name": "signal",
        "data": "signal"
      },
      {
        "name": "auxiliary_signals",
        "data": ["auxiliary"]
      },
      {
        "name": "axes",
        "data": ["x", "y"]
      },
      {
        "name": "title",
        "data": "Some cool title for this data group"
      }
    ],
    "variables": [
      {
        "name": "signal",
        "unit": "counts",
        "label": "Integrated intensity on the detector",
        "data_type": "uint64",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "signal_errors",
        "unit": "counts",
        "label": "Uncertainty in the integrated intensity",
        "data_type": "float64",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "auxiliary",
        "unit": "degrees",
        "label": "The scattering angle of each pixel",
        "data_type": "float32",
        "axes": ["x", "y"],
        "shape": [1000, 1000]
      }],
    "constants": [
      {
        "name": "x",
        "unit": "cm",
        "label": "Position along the x-axis",
        "data_type": "float32",
        "axes": ["x"],
        "shape": [1000],
        "data": {"first":  0.0, "last":  99.9, "size": 1000}
      },
      {
        "name": "y",
        "unit": "cm",
        "label": "Position along the y-axis",
        "data_type": "float32",
        "axes": ["y"],
        "shape": [1000],
        "data": [0.0, 0.1, 0.2, ..., 99.9]
      }
    ]
  }
}
```