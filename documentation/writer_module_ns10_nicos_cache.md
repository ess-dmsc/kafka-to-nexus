# NicosCache writer module

Schema `ns10_cache_entry.fbs` defines the `CacheEntry` with flatbuffer
schema id `ns10`.

NICOS uses it to store any cached value, if Kafka is selected as Kache storage.

## Stream configuration fields

|Name|Type|Required|Description|
---|---|---|---|
topic|string|Yes|The kafka topic to listen to for data.|
source|string|Yes|The source (name) of the data to be written.|
writer_module|string|Yes|The identifier of this writer module (i.e. "ns10").|
chunk_size|int|No|The HDF5 chunk size in nr of elemnts. Defaults to 1024.|
cue_interval|int|No|The interval (in nr of elements/values) at which indices for searching the data should be created. Defaults to 1000.|


## Example

We can write the `CacheEntry` stream to HDF with a child in the
`nexus_structure` like:

```json
{
  "module": "ns10",
  "config": {
    "topic": "nicos.cache.topic",
    "source": "nicos/device/parameter"
  }
}
```

The `source` **must** match the key NICOS uses for the cache. Precisely, `source`
has to be in the form

```
nicos/<device>/<parameter>
```

where the prefix `nicos/` is required, `device` and `parameter` must be `/`
separated and correspond to the names NICOS uses for the device and the specific
parameter.

NOTE: The source name must be lower-case even if the device name isn't. 
E.G NICOS device `motor_Y` becomes `nicos/motor_y/value`.