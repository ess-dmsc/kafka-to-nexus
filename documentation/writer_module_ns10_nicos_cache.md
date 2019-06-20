# NicosCache writer module

Schema `ns10_cache_entry.fbs` defines the `CacheEntry` with flatbuffer
schema id `ns10`.

NICOS uses it to store any cached value, if Kafka is selected as Kache storage.
The value of the cache entry is **ALWAYS** a string.

We can write the `CacheEntry` stream to HDF with a child in the
`nexus_structure` like:

```json
{
  "type": "stream",
  "stream": {
    "topic": "nicos.cache.topic",
    "source": "nicos/device/parameter",
    "writer_module": "ns10",
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
