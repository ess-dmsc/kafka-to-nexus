amthewar# *ev44* Detector event

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "ev44").|
cue_interval|int|No|The interval (in nr of events) at which indices for searching the data should be created. Defaults to 100 million.|
chunk_size|int|No|The HDF5 chunk size in nr of elements. Defaults to 1M.|


### Example

Example `nexus_structure` to write radiation detector events:

```json
{
  "nexus_structure": {
    "children": [
      {
        "module": "ev44",
        "config": {
          "topic": "the_kafka_topic",
          "source": "the_source_name"
        }
      }
    ]
  }
}
```


## Flatbuffer payloads

Messages with pulse information (`reference_time` / `reference_time_index`) but
empty `time_of_flight` contain no events and will not be written to file.


## Written NeXus structure

| Description                                                   | Dimensions | ev44 name                        | NXevent_data name     |
|---------------------------------------------------------------|------------|----------------------------------|-----------------------|
| Array of offsets from pulse time for each event               | `[i]`      | `time_of_flight`                 | `event_time_offset`   |
| Array of pixel IDs                                            | `[i]`      | `pixel_id`                       | `event_id`            |
| Array of pulse times                                          | `[j]`      | `reference_time`                 | `event_time_zero`     |
| Map from each pulse time to the first event of that pulse     | `[j]`      | `reference_time_index`           | `event_index`         |
| Array of timestamps for indexing                              | `[k]`      | (configured in JSON `cue_interval`) | `cue_timestamp_zero`  |
| Array of event indexes corresponding to each timestamp in cue_timestamp_zero | `[k]` | (configured in JSON `cue_interval`) | `cue_index`         |

