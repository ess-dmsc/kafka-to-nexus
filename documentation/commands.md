# Commands

Commands in the form of JSON messages are used to start and stop file writing.

Commands are generally send through Kafka via the broker and topic specified by the
`--command-uri` option; however, commands can also be given in the [configuration
file](Commands via the configuration file).

## Command to start writing

The start command consists of a number of parameters which are defined as key-value pairs in the JSON.
These are:

- cmd: The command name, must be `FileWriter_new`.
- job_id: A unique identifier for the request, a UUID for example.
- broker: The Kafka broker to use for data.
- start_time: The start time in milliseconds (UNIX epoch) for data to be written from. Optional, if not supplied then the Kafka message time is used.
- stop_time: The stop time in milliseconds (UNIX epoch) for data writing to stop. Optional, if not supplied then file writing continues until a stop command is received
- service_id: The identifier for the instance of the file-writer that should handle this command. Optional, only needed if multiple file-writers present.
- abort_on_uninitialised_stream: Whether to abort if the stream cannot be initialised. Optional, default is not to abort but carry on.
- use_hdf_swmr: Whether to use HDF5's Single Writer Multiple Reader (SWMR) capabilities. Optional, default is true. For more on SWMR see [below](Single Writer Multiple Reader).
- file_attributes: Dictionary specifying the details of the file to be written:
    * file_name: The file name
- nexus_structure: Defines the structure of the NeXus file to be written.

An example command with the `nexus_structure` skipped for brevity:

```json
{
  "cmd": "FileWriter_new",
  "job_id": "7119ce9c-1591-11e9-ab14-d663bd873d93",
  "broker": "localhost:9092",
  "start_time": 1547198055,
  "stop_time": 1547200800,
  "service_id": "filewriter1",
  "abort_on_uninitialised_stream": false,
  "use_hdf_swmr": true,
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

Groups are the container mechanism by which HDF5 files are organised; they can be thought of as analogous to directories in a file system. In the file-writer, they can contain datasets, streams, links or, even, other groups in their array of `children`. They can also have attributes defined which are used to provide metadata about the group.

NeXus classes are defined using a group with an attribute named `NX_class` which contains the relevant class name.
Other NeXus-related information can also defined using attributes. See the [NeXus website](https://www.nexusformat.org/) for more information.

The following shows an example of adding a group to a structure:

```JSON
"nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "my_test_group",
        "children": [
          {
            "type": "stream",
            "stream": {
              "type": "double",
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
- type: The type of the data. The possible types are defined in the schema declared in the `writer_module`.
- source: The name of the data source. For example, the EPICS PV name.
- topic: The Kafka topic where the data can be found.

Note: streams are automatically assigned a NeXus class based on the `writer_module`. [NEEDS TO BE CHECKED]

In the following example a dataset is defined:

```JSON
"nexus_structure": {
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
"nexus_structure": {
  "children": [
    {
      "type": "group",
      "name": "group_with_a_link",
      "children": [
        {
          "type": "link",
          "name": "some_link_to_value",
          "target": "../group_with_dataset/some_static_data/values"
        },
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

## Command to stop writing

The stop command consists of a number of parameters which are defined as key-value pairs in the JSON.
These are:

- cmd: The command name, must be `FileWriter_stop`.
- job_id: A unique identifier for the request, a UUID for example. Should be the same as used in the start command.
- stop_time: The stop time in milliseconds (UNIX epoch) for data writing to stop. Optional, if not supplied then the Kafka message time is used.
- service_id: The identifier for the instance of the file-writer that should handle this command. Optional, only needed if multiple file-writers present.

For example:

```json
{
  "cmd": "FileWriter_stop",
  "job_id": "7119ce9c-1591-11e9-ab14-d663bd873d93",
  "stop_time": 1547200800,
  "service_id": "filewriter1",
}
```

## Command to exit the file-writer

The file-writer can be requested to exit.
The parameters for the command are:

- cmd: The command name, must be `FileWriter_stop`.
- service_id: The identifier for the instance of the file-writer that should handle this command. Optional, only needed if multiple file-writers present.

For example:

```json
{
  "cmd": "FileWriter_exit",
  "service_id": "filewriter1",
}
```

### Commands via the configuration file

When running the file-writer is is possible to use a configuration file by specifying `--config-file <file.json>`.
This file can also contain commands, for example: it could be configured to start file-writing immediately.

The syntax for including commands is:

```json
"commands": [
  { "some command": "as discussed above" }
]
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
