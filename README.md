# GilaDB
A toy persistent key-value store inspired by RocksDB. Some info on how it works:
- Based on an LSM Tree, but only goes to Level 0 (multiple full key range SST files). At the top is a MemTable followed
by immutable MemTables, and finally Level 0
- The top MemTable is swapped out once it hits its size limit for a new one
- MemTables are backed by an arena and an AVL Tree serving as an index into the arena
- All writes are also written to a WAL file that gets swapped along with the MemTable
- Dedicated background thread for 'compaction', but since there's only L0 it's really just turning MemTables into SST files
and adding them to L0
- SST files uses a block based format and contain an index block for faster lookup (see SST.kt for full file format)
- supports get, set, delete operations

```kotlin
val gila = GilaDB.open("some/path")
gila.set("foo", "bar")
val r = gila.get("foo")
gila.delete("foo")
```

Made this just for fun and because in another toy project, this time for a distributed KV store, I made a really janky 
embedded db that was basically just a MemTable and thought it would be cool to upgrade it to something a little less
janky that actually persisted to disk. Possible future improvements would be to support more levels in the LSM Tree
and implement an actual compaction strategy, but i'm pausing this project for now.
