# Commands

Commands encoded via [streaming-data-types](https://github.com/ess-dmsc/streaming-data-types)
are used to start and stop file writing.

All commands are sent through Kafka via the topic specified by the
`--command-status-topic` option.

Start commands can also be sent via the topic specified by
`--job-pool-topic`, where a pool of file-writers can be
configured to consume jobs from a common topic.
When a pool is used, the start command typically specifies
a separate topic (in the `control_topic` field) that will be used for further communication related to the job.


## Message types

There are 5 types of messages:

- [pl72 (run start)](https://github.com/ess-dmsc/streaming-data-types/blob/7e80bde7c64f13235ac21f8da9ae86bb40cc2c97/schemas/pl72_run_start.fbs): Command to start file-writing.
- [6s4t (run stop)](https://github.com/ess-dmsc/streaming-data-types/blob/7e80bde7c64f13235ac21f8da9ae86bb40cc2c97/schemas/6s4t_run_stop.fbs): Command to set the stop time of file-writing (**optional, the "run start" message can already contain a stop time**).
- [anws (command answer)](https://github.com/ess-dmsc/streaming-data-types/blob/7e80bde7c64f13235ac21f8da9ae86bb40cc2c97/schemas/answ_action_response.fbs): Reply to start/stop commands (the command can be accepted or rejected).
- [x5f2 (status)](https://github.com/ess-dmsc/streaming-data-types/blob/7e80bde7c64f13235ac21f8da9ae86bb40cc2c97/schemas/x5f2_status.fbs): Periodic status reports sent by the file-writer.
- [wrdn (finished writing)](https://github.com/ess-dmsc/streaming-data-types/blob/7e80bde7c64f13235ac21f8da9ae86bb40cc2c97/schemas/wrdn_finished_writing.fbs): Reports the completion of a file-writing job (the job may have succeeded or failed).


## RunStart command

### Switching to an alternative control topic

If a `control_topic` is specified in the `RunStart` message, the file-writer will, after consuming the `RunStart` message,
switch all subsequent communications regarding the writing job to that topic.

This means that the `command answer`, `status` and `finished writing`
messages will be sent to the topic indicated in the `RunStart` message.

After completing the job, the file-writer will switch back to its
default `command` and `pool` topics.

### Start writing from a template HDF file instead of creating a new file from scratch

If an instrument name is specified in the run start message (pl72), the file-writer will look for a template with static data such as the detector geometry.
If no instrument name is specified it will create a new NeXus file, if the NeXus structure defined in the start message is large, the creation of the file will take a significant amount of time. For example, the DREAM NeXus structure which is ~700MB takes around 10-15 minutes before it can start writing data.

### Defining a NeXus structure in the RunStart command

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
            "module": "f144",
            "config": {
              "dtype": "double",
              "source": "my_test_pv",
              "topic": "my_test_topic"
            }
          }
        ]
      }
    ]
}
```


In the example above, a group called my_test_group is defined which in turn has a (stream) writer module and an attribute defined.

The parameters for the stream/module definition are:

- module: The either the file identifier of a FlatBuffers schema in the [streaming-data-types](https://github.com/ess-dmsc/streaming-data-types) repository that was used to serialise the data or another identifier if there exists multiple writer modules for the same schema.
- dtype: The type of the data. The possible types are defined in the schema declared in the `writer_module`.  **Note** that not all the writer modules take this parameter. 
- source: The name of the data source. For example, the EPICS PV name.
- topic: The Kafka topic where the data can be found.

Note also that some module specific parameters exists. You can find the documentation on individual writer modules in this directory.

A NX_class value will be assigned to the group containing the dataset automatically. 

Attributes should not be assigned at the `"group"` level. The file-writer module owns the group and any attributes must be created from inside module. The module's arguments can be used to pass any
information that needs to be written as an attribute.


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

#### Generated datasets

Static datasets can also be generated from a few parameters, instead of listing all values, using the `arange`, `linspace`, `meshgrid` and `strided` modules.
These are prototypes for possible NeXus array-generator classes.
The first three follow the semantics of the [numpy](https://numpy.org/doc/stable/reference/routines.array-creation.html#numerical-ranges) function of the same name, and the generated values are written to the file as an ordinary dataset.
As for static datasets, attributes are assigned on the same level in the hierarchy as `config`, and an already existing dataset (e.g. from a template file) is not overwritten.

```JSON
{
  "module": "arange",
  "config": {"name": "x", "start": 1, "stop": 11, "step": 1},
  "attributes": {"units": "mm"}
}
```

writes `x` containing `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`: as in numpy, `stop` is excluded.

The parameters for `arange` are:
- name: The name of the dataset in the NeXus file.
- start: The first value. Defaults to 0.
- stop: The end of the interval, which is not included.
- step: The spacing between values, which may be negative but not zero. Defaults to 1.
- dtype: The data type of the dataset. Defaults to int64 if start, stop and step are all integers, otherwise to double.

The parameters for `linspace` are:
- name: The name of the dataset in the NeXus file.
- start: The first value.
- stop: The last value, unless `endpoint` is false.
- num: The number of values. Defaults to 50.
- endpoint: Whether `stop` is the last value. Defaults to true.
- dtype: The data type of the dataset. Defaults to double.

`meshgrid` writes one N-dimensional dataset per input vector, where the vectors are `arange` or `linspace` nodes:

```JSON
{
  "module": "meshgrid",
  "config": {
    "indexing": "xy",
    "vectors": [
      {"module": "arange", "config": {"name": "x", "stop": 3}, "attributes": {"units": "mm"}},
      {"module": "linspace", "config": {"name": "y", "start": 0, "stop": 1, "num": 2}}
    ]
  },
  "attributes": {"units": "m"}
}
```

writes `x` containing `[[0, 1, 2], [0, 1, 2]]` and `y` containing `[[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]]`.

The parameters for `meshgrid` are:
- vectors: The list of `arange` or `linspace` nodes. Each output dataset takes its name and data type from its vector.
- indexing: `"xy"` (Cartesian) or `"ij"` (matrix) indexing, as for numpy. Defaults to `"xy"`.
- sparse: If true, each output has length 1 along all but its own axis. Defaults to false.

Attributes of the `meshgrid` node are written to every output dataset, and then those of each vector node, which replace any with the same name.

`strided` writes an N-dimensional dataset whose values are a linear function of their indices, `start + strides[0]*i0 + strides[1]*i1 + ...`, e.g. detector numbers which run contiguously along a tube but jump between tubes:

```JSON
{
  "module": "strided",
  "config": {"name": "detector_number", "start": 1801, "shape": [3, 100], "strides": [900, 1], "dtype": "int32"}
}
```

writes `detector_number` containing `[[1801, ..., 1900], [2701, ..., 2800], [3601, ..., 3700]]`, as would numpy's `1801 + np.add.outer(np.arange(3) * 900, np.arange(100))`.
A zero stride repeats values along its axis, so pixel offsets can be given one dataset at a time, e.g. `"shape": [3, 100], "strides": [0, -0.002]` for positions which vary along the tubes only.

The parameters for `strided` are:
- name: The name of the dataset in the NeXus file.
- start: The first value, at index zero along every axis. Defaults to 0.
- shape: The length of each axis, as a non-empty list.
- strides: The change in value per step along each axis, as a list of the same length as `shape`. Strides may be negative or zero.
- dtype: The data type of the dataset. Defaults to int64 if start and all strides are integers, otherwise to double.

Each value is calculated from its own indices, so rounding errors do not accumulate over the array.
Unlike for `linspace`, the last value along an axis is the result of a calculation rather than given, so it may differ from an intended value by a rounding error.

Numeric data types are supported, i.e. not `string`.
Values written to an integer data type are rounded down, as by numpy's `linspace`, and a value outside the range of the data type is an error.
A generated dataset may have at most 2<sup>27</sup> elements.
If the configuration is invalid, an error is logged and the dataset is not written.

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
