# DataArray writer module

## Stream configuration fields

<table>
<tr>
<td colspan=2><b>Name</b>   <td><b>Type</b>  <td><b>Description</b>                <td><b>Default</b> <tr>
<td>topic         <td>           <td>string  <td>The kafka topic to listen to for data.          <td> <tr>
<td>source        <td>           <td>string  <td>The source (name) of the data to be written.    <td> <tr>
<td>cue_interval  <td>              <td>int  <td>The number of messages between cues.        <td>1000 <tr>
<td>chunk_size    <td>              <td>int  <td>The HDF5 chunk size.              <td>2<sup>20</sup> <tr>
<td>variables     <td>     <td>list[string]  <td>The names of the Variable datasets to write.  <td>[] <tr>
<td>constants     <td>     <td>list[string]  <td>The names of the Constant datasets to write.  <td>[] <tr>
<td>datasets      <td>       <td>list[dict]  <td>Detailing the variable and constant datasets. <td>[] <tr>
<td>              <td>name       <td>string  <td>The name of the dataset.                        <td> <tr>
<td>              <td>unit       <td>string  <td>The unit along this dimension.            <td>"auto" <tr>
<td>              <td>label      <td>string  <td>The label of the dimension.               <td>"auto" <tr>
<td>              <td>data_type  <td>string  <td>The element type of the written data.     <td>"auto" <tr>
<td>              <td>dims <td>list[string]  <td>The names of the dimensions of the Variable.  <td>[] <tr>
<td>              <td>shape   <td>list[int]  <td>The size of the histogram in each dimension.  <td>[] <tr>
<td>              <td>data <td>list[number]  <td>The constant values of the Variable.          <td>[] <tr>
</table>

### Notes on `variables` and `constants`
The `variables` and `constants` configuration fields are lists of the names of buffered DataArray Variables to write into the HDF5 file.
A DataArray buffer may contain any number of Variables, each of which comprises

| Field     | Type         | Description                                 | Constant |
|-----------|--------------|---------------------------------------------|----------|
| name      | string       | The name of the Variable                    | Yes      |
| unit      | string       | The unit of the contained values (optional) | Yes      |
| label     | string       | A label describing the Variable (optional)  | Yes      |
| data_type | string       | The type of the contained values            | Yes      |
| dims      | list[string] | The names of the dimensions of the Variable | Yes      |
| shape     | list[int]    | The size of the Variable in each dimension  | Yes      |
| data      | *            | The values of the Variable                  | Maybe    |

Most fields in the buffered messages (received in a single write-request) must be constant.
The `data` field is the only exception, and may contain different values in each message if 
the name of the Variable is listed in the `variables` configuration field.

Since the contained values may change, `variables` are written to time-series datasets in the HDF5 file.
In contrast, `constants` must contain the same value in every message, so are written to scalar datasets in the HDF5 file.

### Notes on `datasets` sub-fields
The buffered Variable objects contain all information required to reconstruct full arrays and write them to HDF5.
However, it may be necessary to _confirm_ that the buffered information matches expectations,
so the `datasets` configuration field is provided to specify the expected Variable contents for buffered objects
listed in `variables` and `constants`.

The `datasets` configuration field is a list of dictionaries where each dictionary has contentes listed above.
If a dictionary is provided with missing or default values, the behavior is as follows:

| Field     | Type         | Missing ->  | Default ->               |
|-----------|--------------|-------------|--------------------------|
| name      | string       | Error       |                          |
| unit      | string       | Default     | First message            |
| label     | string       | Default     | First message            |
| data_type | string       | Default     | First message            |
| dims      | list[string] | Default     | First message            |
| shape     | list[int]    | Default     | First message            |
| data      | *            | Default     | First message \| ignored |

If a dictionary is provided with a name and otherwise all default or missing values, it has the same effect
as if the dictionary was omitted from the `datasets` configuration field.

The `data` field is special, in that it can _either_ be a vector of values
or a object specification of the form `{"first": number, "last": number, "size": number}`,
in which case it is interpreted as a range of values from `first` to `last` in steps of (`last` - `first`) / (`step` - 1).

### Notes on required values
Any configuration field with a default value is optional, and may be omitted.

## Examples

Schema `da00_dataarray_generated.h` defines the `da00_DataArray` with flatbuffer schema id `da00`.

It is used to exchange multidimensional data, and optionally errors and axis descriptions.

We can write the `da00_DataArray` stream to HDF with a child in the `nexus_structure` like:

```json
{
  "module": "da00",
  "config": {
    "topic": "topic.with.multiple.sources",
    "source": "some_dataarray_producer"
  }
}
```

This will follow all defaults, which writes all Variables present _in the first message_ to the HDF5 file as
time-series datasets.

A slightly more useful configuration would specify the variable and constant Variable objects that should be written


```json
{
  "module": "da00",
  "config": {
    "topic": "other.topic.with.multiple.sources",
    "source": "another_dataarray_producer",
    "variables": ["var1", "var2", "var3"],
    "constants": ["const1", "const2", "const3"]
  }
}
```

Finally, a somewhat realistic configuration would specify the expected contents of the buffered Variable objects

```json
{
  "module": "da00",
  "config": {
    "topic": "topic.with.multiple.sources",
    "source": "some_dataarray_producer",
    "variables": ["signal", "signal_error", "auxiliary"],
    "constants": ["x", "y"],
    "datasets": [
      {
        "name": "signal",
        "unit": "counts",
        "label": "Integrated intensity on the detector",
        "data_type": "uint64",
        "dims": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "signal_error",
        "unit": "counts",
        "label": "Uncertainty in the integrated intensity",
        "data_type": "float64",
        "dims": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "auxiliary",
        "unit": "degrees",
        "label": "The scattering angle of each pixel",
        "data_type": "float32",
        "dims": ["x", "y"],
        "shape": [1000, 1000]
      },
      {
        "name": "x",
        "unit": "cm",
        "label": "Position along the x-axis",
        "data_type": "float32",
        "dims": ["x"],
        "shape": [1000],
        "data": {"first":  0.0, "last":  99.9, "size": 1000}
      },
      {
        "name": "y",
        "unit": "cm",
        "label": "Position along the y-axis",
        "data_type": "float32",
        "dims": ["y"],
        "shape": [1000],
        "data": [0.0, 0.1, 0.2, ..., 99.9]
      }
    ]
  }
}
```