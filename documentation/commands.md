Commands in the form of JSON messages are used to start and stop file writing.
Commands can be send through Kafka via the broker and topic specified by the
`--command-uri` option.  Commands can also be given in the configuration
file specified by `--config-file <file.json>` (see [commands in config file](#commands-can-be-given-in-the-configuration-file-as-well)).

In the command, the `nexus_structure` defines the HDF hierarchy.
The `nexus_structure` represents the HDF root object.  The following example
shows how the HDF tree can be constructed using `children`.
A child of type `stream` is a marker that a `HDFWriterModule` will insert
the data into from a Kafka stream at that point in the hierarchy.  The options under
the key `stream` specify the stream details containing, at least, the topic, the source name and
the `HDFWriterModule` which should be used for writing.
Depending on the `HDFWriterModule`, there will be more options specific to the
`HDFWriterModule`.

#### Command to start writing a file

Further documentation:

- [Groups](docs/groups.md)
- [~~Datasets~~ documentation not yet written]()
- [Attributes](docs/attributes.md)
- [~~File Attributes~~ documentation not yet written]()
- [~~Streams~~ documentation not yet written]()

Example command to start streaming:

```json
{
  "nexus_structure": {
    "children": [
      {
        "type": "group",
        "name": "for_example_motor_0000",
        "attributes": {
          "NX_class": "NXinstrument"
        },
        "children": [
          {
            "type": "stream",
            "attributes": {
              "this_will_be_a_double": 0.123,
              "this_will_be_a_int64": 123
            },
            "stream": {
              "topic": "topic.with.multiple.sources",
              "source": "for_example_motor",
              "writer_module": "f142",
              "type": "float",
              "array_size": 4
            }
          },
          {
            "type": "dataset",
            "name": "some_static_dataset",
            "values": 42.24,
            "attributes": {
              "units": "Kelvin"
            }
          },
          {
            "type": "dataset",
            "name": "some_more_explicit_static_dataset",
            "dataset": {
              "space": "simple",
              "type": "uint64",
              "size": ["unlimited", 5, 6]
            },
            "values": [[[0, 1, 2, 3, 4, 5], ["…"], "…"], ["…"], "…"]
          },
          {
            "type": "dataset",
            "name": "string_scalar",
            "dataset": {
              "type": "string"
            },
            "values": "the-scalar-string"
          },
          {
            "type": "dataset",
            "name": "string_3d",
            "dataset": {
              "type": "string",
              "size": ["unlimited", 3, 2]
            },
            "values": [
              [
                ["string_0_0_0", "string_0_0_1"],
                ["string_0_1_0", "string_0_1_1"],
                ["string_0_2_0", "string_0_2_1"]
              ],
              [
                ["string_1_0_0", "string_1_0_1"],
                ["string_1_1_0", "string_1_1_1"],
                ["string_1_2_0", "string_1_2_1"]
              ]
            ]
          },
          {
            "type": "dataset",
            "name": "string_fixed_length_1d",
            "dataset": {
              "type":"string",
              "string_size": 32,
              "size": ["unlimited"]
            },
            "values": ["the-scalar-string", "another-one"],
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
        ]
      }
    ]
  },
  "file_attributes": {
    "file_name": "some.h5"
  },
  "cmd": "FileWriter_new",
  "job_id": "unique-identifier",
  "broker": "localhost:9092",
  "start_time": "[OPTIONAL] timestamp (int) in milliseconds",
  "stop_time": "[OPTIONAL] timestamp (int) in milliseconds",
  "service_id": "[OPTIONAL] the_name_of_the_instance_which_should_interpret_this_command",
  "abort_on_uninitialised_stream": false,
  "use_hdf_swmr": true
}
```
By default, the file-writer will continue to write a file even when a stream fails to initialise.
In order to abort writing instead `set abort_on_uninitialised_stream` to `true`.

#### Command to exit the file-writer

```json
{
  "cmd": "FileWriter_exit",
  "service_id": "[OPTIONAL] the_name_of_the_instance_which_should_interpret_this_command"
}
```

#### Command to stop a file being written

```json
{
  "cmd": "FileWriter_stop",
  "job_id": "job-unique-identifier",
  "stop_time" : "[OPTIONAL] timestamp (int) in milliseconds",
  "service_id": "[OPTIONAL] the_name_of_the_instance_which_should_interpret_this_command"
}
```

#### Commands can be given in the configuration file as well

```json
{
  "commands": [
    { "some command": "as discussed above" }
  ]
}
```


### Links within the HDF File

HDF links are created by placing a child of type `link` in the list of children
of a group.  Links are created as hard links at the end of file writing.  For
example:

```
"nexus_structure": {
  "children": [
    {
      "type": "group",
      "name": "extra_group",
      "children": [
        {
          "type": "link",
          "name": "some_link_to_value",
          "target": "../a_group/a_subgroup/value"
        },
        {
          "type": "link",
          "name": "some_absolute_link_to_value",
          "target": "/a_group/a_subgroup/value"
        }
      ]
    },
    {
      "type": "group",
      "name": "a_group",
      "children": [
        {
          "type": "group",
          "name": "a_subgroup",
          "children": [
            {
              "type": "dataset",
              "name": "value",
              "values": 42.24
            }
          ]
        }
      ]
    }
  ]
}
```

### Options for the f142 writer module

- `type`: The data type contained in the FlatBuffer. Can be `int8` to `int64`, `uint8`, `float` or `double`.
- `array_size`: The size of the array. Scalar if not specified or `0`.
- `store_latest_into`: (optional) Name of the dataset into which the latest
  received value should be stored on file-writer close.


### Single Writer, Multiple Reader support

The file-writer can use HDF's Single Writer Multiple Reader feature (SWMR).
SWMR-mode writing is enabled by default.

To disable SWMR, set `use_hdf_swmr` to false in the `FileWriter_new` command:

To read and write HDF files which use the SWMR feature requires HDF5 version 1.10 or higher.
One can also use the HDF5 tool `h5repack` with the `--high` option to convert the file into a HDF5 1.8 compatible version.  Please refer to see the `h5repack` documentation for more information.

Please note, the HDF documentation warns that:

"The HDF5 file that is accessed by SWMR HDF5 applications must be located on a
file system that complies with the POSIX `write()` semantics."

Also:

"The writer is not allowed to modify or append to any data items containing
variable-size datatypes (including string and region references datatypes)."
