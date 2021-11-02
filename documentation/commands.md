# Commands

Commands in the form of JSON messages are used to start and stop file writing.

Commands are sent through Kafka via the broker and topic specified by the
`--command-uri` option.

Note: some example commands can be found in the system tests.

## Command to start writing

The start command consists of a number of parameters which are defined as key-value pairs in the JSON.

Required:

- cmd: The command name, must be `filewriter_new`
- job_id: A unique identifier for the request, a UUID for example
- broker: The Kafka broker to use for data.
- file_attributes: Dictionary specifying the details of the file to be written:
    * file_name: The file name
- nexus_structure: Defines the structure of the NeXus file to be written

Optional:

- start_time: The start time in milliseconds (UNIX epoch) for data to be written from. If not supplied then the timestamp of the Kafka message containing the start command is used.
- stop_time: The stop time in milliseconds (UNIX epoch) for data writing to stop. If not supplied then file writing continues until a stop command is received
- service_id: The identifier for the instance of the file-writer that should handle this command. Only needed if multiple file-writers present.


An example command with the `nexus_structure` skipped for brevity:

```json
{
  "cmd": "FileWriter_new",
  "job_id": "7119ce9c-1591-11e9-ab14-d663bd873d93",
  "broker": "localhost:9092",
  "start_time": 1547198055000,
  "stop_time": 1547200800000,
  "service_id": "filewriter1",
  "file_attributes": {
    "file_name": "my_nexus_file.h5"
  } ,
  "nexus_structure": {
    # Skipped for brevity
  }
}
```

### Defining a NeXus structure

The `nexus_structure` represents the HDF root object of the file to be written.
For more detailed information on all aspects of HDF5 see the [official HDF5 documentation](https://portal.hdfgroup.org/display/HDF5/HDF5).
For more information about NeXus see the [NeXus website](https://www.nexusformat.org/).

#### Groups

Groups are the container mechanism by which HDF5 files are organised; they can be thought of as analogous to directories in a file system. In the file-writer, they can contain datasets, streams, links or, even, other groups in their array of `children`. They can also have attributes defined which are used to provide metadata about the group; see [below](Attributes) for more information on attributes.

NeXus classes are defined using a group with an attribute named `NX_class` which contains the relevant class name.
Other NeXus-related information can also defined using attributes. See the [NeXus website](https://www.nexusformat.org/) for more information.

The following shows a simple example of a structure of nested groups:

```JSON
"nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "my_test_group",
        "children": [
          {
            "type": "group",
            "name": "my_nested_group_1"
          },
          {
            "type": "group",
            "name": "my_nested_group_2",
            "attributes": [
              {
                "NX_class": "NXlog",
                "unit": "blintz"
              }
            ]
          }
        ],
        "attributes": [
          {
            "NX_class": "NXentry",
            "origin": "spaaaaace"
          }
        ]
      }
    ]
}
```

In the above example, the attributes are assigned to the groups that are defined on the same level.

#### Data streams *alt.* writer modules

The (stream/writer) module definition instructs the file-writer that there is data available which should be written to the containing group. The example below illustrates how this can be done.

```JSON
"nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "my_test_group",
        "children": [
          {
            "module": "f142",
            "config": {
              "dtype": "double",
              "source": "my_test_pv",
              "topic": "my_test_topic"
            }
          }
        ],
        "attributes": [
          {
            "NXclass": "NXtransformation",
            "unit": "mm"
          }
        ]
      }
    ]
}
```


In the example above, a group called my_test_group is defined which in turn has a (stream) writer module and an attribute defined.

The parameters for the stream/module definition are:

- module: The either the file identifier of a FlatBuffers schema in the [streaming-data-types](https://github.com/ess-dmsc/streaming-data-types) repository that was used to serialise the data or another identifier if there exists multiple writer modules for the same schema.
- dtype: The type of the data. The possible types are defined in the schema declared in the `writer_module`. Program allows both `type` and `dtype` spellings for better Python usability. **Note** that not all the writer modules take this parameter. 
- source: The name of the data source. For example, the EPICS PV name.
- topic: The Kafka topic where the data can be found.

Note also that some module specific parameters exists. You can find the documentation on individual writer modules in this directory.

A NX_class value will be assigned to the group containing the dataset automatically. If you want to override this assignment, set the corresponding attribute of the group.


#### Static datasets

Static datasets use the same general structure as writer-modules.

```JSON
"nexus_structure": {
    "children": [
      {
        "module": "dataset",
        "config": {
          "name": "some_static_data",
          "dtype": "int64",
          "values": [
            [0, 1, 3 ],
            [2, 2, 1 ]
          ]
        },
        "attributes": {
          "NX_class": "NXlog"
        }
      }
    ]
}
```

In the above example, a dataset is defined which contains some values in two dimenstional array.

The parameters for the dataset definition are:
- name: The name of the dataset in the NeXus file.
- values: The data to be stored in the dataset. Note that the shape of the dataset is determined from the layout of the data in the JSON structure.
- dtype: The data type of the individual elements in the dataset. Defaults to double.

In this example, attributes are assigned to the dataset on the same level in the hierarchy.

#### Links

In HDF5 links are used to link a group to objects in other groups in a manner similar to links on a filesystem.
The links are created as hard links when the file is closed. A link is defined in the `children` of a group.

For example:

```JSON
"nexus_structure": {
  "children": [
    {
      "type": "group",
      "name": "group_with_a_link",
      "children": [
        {
          "module": "link",
          "config": {
            "name": "some_link_to_value",
            "source": "../group_with_dataset/some_static_data/values"
        }
        },
      ]
    },
    {
      "type": "group",
      "name": "group_with_dataset",
      "children": [
        {
          "module": "dataset",
          "config": {
            "name": "some_static_data",
            "dtype": "int64",
            "values": [ 0, 1, 3, 2, 2 ]
          },
          "attributes": {
            "NX_class": "NXlog"
          }
        }
      ]
    }
  ]
}
```

## Command to stop writing

The stop command consists of a number of parameters which are defined as key-value pairs in the JSON.

Required:

- cmd: The command name, must be `filewriter_stop`
- job_id: A unique identifier for the request, a UUID for example. Should be the same as used in the start command

Optional:

- stop_time: The stop time in milliseconds (UNIX epoch) for data writing to stop. If not supplied then the Kafka message time is used
- service_id: The identifier for the instance of the file-writer that should handle this command. Only needed if multiple file-writers present

For example:

```json
{
  "cmd": "FileWriter_stop",
  "job_id": "7119ce9c-1591-11e9-ab14-d663bd873d93",
  "stop_time": 1547200800000,
  "service_id": "filewriter1"
}
```

## Single Writer Multiple Reader

The file-writer can use HDF5's Single Writer Multiple Reader feature (SWMR) which is enable by default.

To read and write HDF files which use the SWMR feature requires HDF5 version 1.10 or higher.
One can also use the HDF5 tool `h5repack` with the `--high` option to convert the file into a HDF5 1.8 compatible version.  Please refer to see the `h5repack` documentation for more information.

Please note, the HDF documentation warns that:

"The HDF5 file that is accessed by SWMR HDF5 applications must be located on a
file system that complies with the POSIX `write()` semantics."

Also:

"The writer is not allowed to modify or append to any data items containing
variable-size datatypes (including string and region references datatypes)."

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
    "dtype": "string",
  },
  {
    "name": "array_attribute",
    "values": [1, 2, 3],
    "dtype": "uint32"
  }
]
```

Note: The two methods for defining attributes (key-value pairs and dictionaries) can not be mixed in the same set of attributes.
