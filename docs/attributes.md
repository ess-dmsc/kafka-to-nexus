# Attributes

[Attributes](https://support.hdfgroup.org/HDF5/doc1.6/UG/13_Attributes.html) are
metadata which can be attached to groups or datasets. They are added using the
`attributes` keyword with the value as either an object or an array. If an
object is provided it contains attribute names and values as key-value pairs.

## Attributes object

Here is a dataset with an `attributes` object as an example:

```json

{
  "type": "dataset",
  "name": "some_dataset",
  "values": 42.24,
  "attributes": {
    "units": "Kelvin",
    "error": 0.02
  }
}

```

In this case the type which should be used in the NeXus file for each attribute
value is inferred by the file writer. The other limitation is that attribute
values which are arrays are not supported. If these are required an
`attributes` array must be used instead.

## Attributes array

Using an array for the `attributes` is more verbose but allows specification
of types and also for attributes with an array of values. Specifying
type is optional in the case of scalars, but compulsory for arrays.

```json
{
  "type": "group",
  "name": "group_with_attributes_array",
  "attributes": [
  {
    "name": "scalar_attribute",
    "values": 42
  },
  {
    "name": "vector_attribute",
    "values": [1, 2, 3],
    "type": "uint32"
  }
  ]
}
``` 
