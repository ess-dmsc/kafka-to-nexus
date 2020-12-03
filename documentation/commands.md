# Commands

Commands are in the form of flatbuffers as defined in [this repository](https://github.com/ess-dmsc/streaming-data-types).
We strongly recommend that you do not use this repo directly but instead make use of the [*file-writer-control*](https://github.com/ess-dmsc/file-writer-control) Python library.

Commands are sent through Kafka via the broker and topic specified by the
`--command-status-uri` command line argument. Additionally, it is also possible to send a start-job-command to a "job-pool" topic. This topic is set with the `--job-pool-uri` argument.

### Defining a NeXus structure

One of the flatbuffer fields in the start command is the `nexus_structure`, which represents the layout/directory tree of the HDF file created by the file-writer.
For more detailed information on all aspects of HDF5 see the [official HDF5 documentation](https://portal.hdfgroup.org/display/HDF5/HDF5).
For more information about NeXus see the [NeXus website](https://www.nexusformat.org/).

Groups are the container mechanism by which HDF5 files are organised; they can be thought of as analogous to directories in a file system. In the file-writer, they can contain datasets, streams, links or, even, other groups in their array of `children`. They can also have attributes defined which are used to provide metadata about the group; see [below](#Attributes) for more information on attributes.

NeXus classes are defined using a group with an attribute named `NX_class` which contains the relevant class name.
Other NeXus-related information can also defined using attributes. See the [NeXus website](https://www.nexusformat.org/) for more information.

The following shows an example of adding a group to a structure:

```JSON
{
    "children": [
      {
        "type": "group",
        "name": "my_test_group",
        "children": [
          {
            "type": "stream",
            "stream": {
              "dtype": "double",
              "writer_module": "f142",
              "source": "my_test_pv",
              "topic": "my_test_topic"
            }
          }
        ],
        "attributes": [
          {
            "name": "units",
            "values": "ms"
          }
        ]
      }
    ]
}
```


In the example above, a group called my_test_group is defined which in turn has a stream and an attribute defined.
The stream definition instructs the file-writer that there is data available which should be written to the containing group.
The parameters for the stream definition are:

- writer_module: The FlatBuffers schema in the [streaming-data-types](https://github.com/ess-dmsc/streaming-data-types) repository that was used to serialise the data.
- type/dtype: The type of the data. The possible types are defined in the schema declared in the `writer_module`. Program allows both `type` and `dtype` spellings for better python usability.
- source: The name of the data source. For example, the EPICS PV name.
- topic: The Kafka topic where the data can be found.

Note: some streams are automatically assigned a NeXus class based on the `writer_module` while some need a NeXus class declaring.

In the following example a dataset is defined:

```JSON
{
    "children": [
      {
        "name": "some_static_data",
        "type": "dataset",
        "dataset": {
          "size": [
            2,
            3
          ],
          "type": "int64"
        },
        "values": [
          [
            0,
            1,
            3
          ],
          [
            2,
            2,
            1
          ]
        ],
        "attributes": {
          "NX_class": "NXlog"
        }
      }
    ]
}
```

In the above example a dataset is defined which contains some values in a 1-D array.
The parameters for the dataset definition are:

- dataset: Defines information about the dataset:
  * size: The shape of the data. Can be scalar, 1-D, 2-D, and so on.
- values: Contains the actual data.

The attributes are used to define the NeXus class as log data.

In HDF5 links are used to link a group to objects in other groups in a manner similar to links on a filesystem.
The links are created as hard links when the file is closed. A link is defined in the `children` of a group.

For example:

```JSON
{
  "children": [
    {
      "type": "group",
      "name": "group_with_a_link",
      "children": [
        {
          "type": "link",
          "name": "some_link_to_value",
          "target": "../group_with_dataset/some_static_data/values"
        }
      ]
    },
    {
      "type": "group",
      "name": "group_with_dataset",
      "children": [
        {
          "name": "some_static_data",
          "type": "dataset",
          "dataset": {
            "size": [
              5
            ],
            "type": "int64"
          },
          "values": [
            0,
            1,
            3,
            2,
            2
          ],
          "attributes": {
            "NX_class": "NXlog"
          }
        }
      ]
    }
  ]
}
```

## Attributes

Attributes are used to define metadata about the data object.
NeXus also uses them to define NeXus classes for groups.

Simple attributes are defined as key-value pairs, for example:

```json
"attributes": {
  "units": "Kelvin",
  "error": 0.02
}
```
Where possible the file-writer will try to infer the type of the data, in this example: a float.

For strings and arrays it is necessary to specify additional supporting information, thus these attributes are defined using a dictionary of key-value pairs.

Both strings and arrays need a `type` to be defined. Strings also require the `string_size` and, optionally, `encoding` to be defined. If no encoding is defined then UTF-8 is assumed.

For example:

```json
"attributes": [
  {
    "name": "some_string_attribute",
    "values": "some_string_value",
    "type": "string",
    "encoding": "ascii",
    "string_size": 17
  },
  {
    "name": "array_attribute",
    "values": [1, 2, 3],
    "type": "uint32"
  }
]
```

Note: The two methods for defining attributes (key-value pairs and dictionaries) can not be mixed in the same set of attributes.
