# Tantivy Object Store
This repo contains an implementation of a `tantivy::directory::Directory` using an `object_store::ObjectStore`.

This implementation supports both read and write, but does not implement locking or file watch. The index building process is responsible for making sure that no two concurrent builder runs at the same time

A few notable behavior differences from tantivy's directory implementations:

## Versioning
Tantivy uses a file called `meta.json` which is a list of all the files that make up the index, effectively keeping track of a snapshot of the index. However, vanilla tantivy doesn't support versioning, meaning every time we update the index, meta.json is overwritten. This PR allows the caller to set a read_version and write_version. These version numbers are appended to the end of the file name when caller attempts to atomic_read or atomic_write.

### Copy on Write (CoW)
When creating a `ObjectStoreDirectory`, user may set `read_version` and `write_version`. `read_version` is used when user calls `atomic_read`. Instead of reading `meta.json`, we will try to read `meta.json.{read_version}`. Same when user calls `atomic_write`, we will try to write `meta.json.{write_version}`

NOTE: The `write_version` take precedence over `read_version`. This means, after first write, `atomic_read` will read from `meta.json.{write_version}` NOT `meta.json.{read_version}`. This is needed because tantivy modifies `meta.json` file quite a few times during indexing, the CoW impl here needs have read-after-write consistency.

## Index Reloading
This implementation does not support reloading. If a `watch` callback is registered, the callback will never be called. User needs to handle reloading via other mechanisms for now.

It maybe possible to use something like object store's native change notification to trigger reload, but that's for future work. 

## Deletion
Since tantivy attempts to garbage collect and merge index files during indexing, we had to change `delete` operation to noop. This is because we don't want tantivy to garbage collect files from past versions, as those files maybe in use by other readers. We will implement a garbage collection processes separately.

## Threading
This implementation contains a `tokio::Runtime` for running the IO jobs. This means, when calling functions from this implementation from inside another tokio runtime the caller should always use `tokio::task::spawn_blocking` so the task can be scheduled on a thread without tokio runtime. (This is needed because nesting tokio runtimes causes panic)

## Concurrency Safety
This implementation is concurrency safe within a single instance, as the `atomic_read|write` mechanism is a lock object in the returned trait object.

## Performance
TBD
