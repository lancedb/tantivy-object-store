// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Object store (S3, AWS, etc.) support for tantivy.

use async_trait::async_trait;
use log::debug;
use object_store::{local::LocalFileSystem, ObjectStore};
use std::{
    fs::File,
    io::Write,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tantivy::{
    directory::{
        self,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
        AntiCallToken, Directory, DirectoryLock, FileHandle, OwnedBytes, TerminatingWrite,
        WatchCallback, WatchHandle, WritePtr,
    },
    HasLen,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

#[derive(Debug, Clone)]
struct ObjectStoreDirectory {
    store: Arc<dyn ObjectStore>,
    base_path: String,
    read_version: Option<u64>,
    write_version: u64,

    cache_loc: Arc<PathBuf>,

    local_fs: Arc<LocalFileSystem>,

    rt: Arc<tokio::runtime::Runtime>,
    atomic_rw_lock: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct ObjectStoreFileHandle {
    store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    // We need to store this becasue the HasLen trait doesn't return a Result
    // We need to do the IO at construction time
    len: usize,

    rt: Arc<tokio::runtime::Runtime>,
}

impl ObjectStoreFileHandle {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
        len: usize,
        rt: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            store,
            path,
            len,
            rt,
        }
    }
}

impl HasLen for ObjectStoreFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

#[async_trait]
impl FileHandle for ObjectStoreFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        let handle = self.rt.handle();
        handle.block_on(async { self.read_bytes_async(range).await })
    }

    #[doc(hidden)]
    async fn read_bytes_async(&self, byte_range: Range<usize>) -> std::io::Result<OwnedBytes> {
        debug!("read_bytes_async: {:?} {:?}", self.path, byte_range);
        let bytes = self.store.get_range(&self.path, byte_range).await?;

        Ok(OwnedBytes::new(bytes.to_vec()))
    }
}

// super dumb implementation of a write handle
// write to local and upload at once
struct ObjectStoreWriteHandle {
    store: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    local_path: Arc<PathBuf>,

    write_handle: File,
    shutdown: AtomicBool,
    rt: Arc<tokio::runtime::Runtime>,
}

impl ObjectStoreWriteHandle {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        location: object_store::path::Path,
        cache_loc: Arc<PathBuf>,
        rt: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self, std::io::Error> {
        let local_path = cache_loc.join(location.as_ref());
        debug!("creating write handle for {:?}", local_path);
        let path = Path::new(&local_path);
        // create the necessary dir path for caching
        std::fs::create_dir_all(path.parent().ok_or(std::io::Error::new(
            std::io::ErrorKind::Other,
            "unable to create parent dir for cache",
        ))?)?;
        let f = File::create(local_path.clone())?;

        Ok(Self {
            store,
            location,
            local_path: Arc::new(local_path),
            write_handle: f,
            shutdown: AtomicBool::new(false),
            rt,
        })
    }
}

impl Write for ObjectStoreWriteHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write handle has been shutdown",
            ));
        }

        self.write_handle.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "write handle has been shutdown",
            ));
        }

        self.write_handle.flush()
    }
}

impl TerminatingWrite for ObjectStoreWriteHandle {
    fn terminate_ref(&mut self, _: AntiCallToken) -> std::io::Result<()> {
        let res = self.flush();
        self.shutdown.store(true, Ordering::SeqCst);

        let result: Result<(), std::io::Error> = self.rt.block_on(async {
            let mut f = tokio::fs::File::open(self.local_path.as_path()).await?;

            let (_, mut sink) = self.store.put_multipart(&self.location).await?;
            // 1 mb blocks
            let mut buf = vec![0; 1024 * 1024];

            loop {
                let n = f.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                sink.write_all(&buf[..n]).await?;
            }

            sink.shutdown().await?;

            Ok(())
        });

        result?;

        res
    }
}

impl ObjectStoreDirectory {
    fn to_object_path(&self, path: &Path) -> Result<object_store::path::Path, std::io::Error> {
        let p = path
            .to_str()
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "non-utf8 path",
            ))?
            .to_string();

        Ok(object_store::path::Path::from(format!(
            "{}/{}",
            self.base_path.clone(),
            p
        )))
    }

    fn head(&self, path: &Path) -> Result<object_store::ObjectMeta, OpenReadError> {
        let location = self
            .to_object_path(path)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;
        let handle = self.rt.handle();
        handle
            .block_on(async { self.store.head(&location).await })
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    OpenReadError::FileDoesNotExist(path.to_path_buf())
                }
                _ => OpenReadError::wrap_io_error(
                    std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)),
                    path.to_path_buf(),
                ),
            })
    }
}

trait Lock: Send + Sync + 'static {}
struct NoOpLock {}
impl NoOpLock {
    pub fn new() -> Box<Self> {
        Box::new(Self {})
    }
}
impl Lock for NoOpLock {}

impl Directory for ObjectStoreDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        debug!("get_file_handle({:?})", path);
        let location = self
            .to_object_path(path)
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?;

        // Check if the file is in local upload cache
        let cache_path = self.cache_loc.join(location.as_ref());
        let cache_path = object_store::path::Path::from(cache_path.to_string_lossy().as_ref());
        if let Ok(meta) = self
            .rt
            .block_on(async { self.local_fs.head(&cache_path).await })
        {
            debug!("upload cache hit: {:?}", path);
            return Ok(Arc::new(ObjectStoreFileHandle::new(
                self.local_fs.clone(),
                cache_path,
                meta.size,
                self.rt.clone(),
            )));
        }

        let len = self.head(path)?.size;

        Ok(Arc::new(ObjectStoreFileHandle::new(
            self.store.clone(),
            location,
            len,
            self.rt.clone(),
        )))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        debug!("atomic_read({:?})", path);
        if path != Path::new("meta.json") && path != Path::new(".managed.json") {
            // Just blow up
            unimplemented!("Only meta.json is supported, but got {:?}", path)
        }

        // Inject versioning into path -- we want to enforce CoW here
        // if the write verison exist, read version has no effect
        // if the write version doesn't exist we read from the old version

        let buf = path.to_string_lossy();
        let path_str = format!("{}.{}", buf, self.write_version);

        // Found write version already valid, read from the write version
        if let Ok(f) = self.get_file_handle(Path::new(&path_str)) {
            return Ok(f
                .read_bytes(0..f.len())
                .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?
                .to_vec());
        }

        // No read version to copy from, return DNE
        if self.read_version.is_none() {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }

        let buf = path.to_string_lossy();
        let path_str = format!(
            "{}.{}",
            buf,
            self.read_version.expect("already checked exists")
        );
        let path = Path::new(&path_str);

        let f = self.get_file_handle(path)?;
        Ok(f.read_bytes(0..f.len())
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_path_buf()))?
            .to_vec())
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, OpenReadError> {
        match self.head(path) {
            Ok(_) => Ok(true),
            Err(OpenReadError::FileDoesNotExist(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // NOTE: the only thing that needs atomic write is the meta.json file
    // we add versioning here in this interface to load different versions
    // of the meta.json file at Directory construction time

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        debug!("atomic_write({:?})", path);
        if path != Path::new("meta.json") && path != Path::new(".managed.json") {
            // Just blow up
            unimplemented!("Only meta.json is supported")
        }

        // Inject versioning into path
        let buf = path.to_string_lossy();
        let path_str = format!("{}.{}", buf, self.write_version);
        let path = Path::new(&path_str);

        let location = self.to_object_path(path)?;

        debug!("true location: {:?}", location);

        self.rt.handle().block_on(async {
            // Lock so no one can write at the same time
            let _ = self.atomic_rw_lock.lock().await;
            self.store
                .put(&location, bytes::Bytes::from(data.to_vec()))
                .await
        })?;

        Ok(())
    }

    fn delete(&self, _: &Path) -> Result<(), DeleteError> {
        // Don't actually garbage collect since we want to have versioning of the meta.json file
        Ok(())
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        debug!("open_write({:?})", path);
        let location = self
            .to_object_path(path)
            .map_err(|e| OpenWriteError::wrap_io_error(e, path.to_path_buf()))?;

        debug!("true location: {:?}", location);

        let write_handle = Box::new(
            ObjectStoreWriteHandle::new(
                self.store.clone(),
                location,
                self.cache_loc.clone(),
                self.rt.clone(),
            )
            .map_err(|e| OpenWriteError::wrap_io_error(e, path.to_path_buf()))?,
        );

        Ok(WritePtr::new(write_handle))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        // Noop as synchronization is handled by the object store
        Ok(())
    }

    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> {
        // We have reload mechanism from else where in the system
        // A sinlge index load will always be immutable
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _: &directory::Lock) -> Result<DirectoryLock, LockError> {
        // We will manually garueentee index RW safety
        Ok(DirectoryLock::from(NoOpLock::new()))
    }
}

/// Create a new object store directory that implements CoW for the meta.json file
///
/// # Arguments
///
/// * store - An object store object
///
/// * base_path - The base path to store the index, relative to the object store root
///
/// * read_version - The version of the meta.json file to read from if a write version hasn't been created.
/// Can not be greater than the write version. Has no effect if there is a write version. Index will be
/// built from scratch if None is provided.
///
/// * write_version - The version of the meta.json file to write to
///
/// * cache_loc - The location to cache the meta.json file. If not provided, a random UUID will be used.
/// This string is used for caching uploaded artifacts under /tmp/{cache_loc} directory
///
/// * rt - An optional tokio runtime to use for async operations. If not provided, a new runtime will be created.
/// NOTE: if you already run from an async context, dropping the returned Directory will cause the runtime to panic
/// as it will attempt to shutdown the runtime inside an async context.
///
/// # Example
/// ```
/// use std::sync::Arc;
///
/// use object_store::local::LocalFileSystem;
/// use tantivy::{Index, IndexSettings, schema::{Schema, STORED, STRING, TEXT}};
/// use tantivy_object_store::new_object_store_directory;
///
/// let store = Arc::new(LocalFileSystem::new());
/// let base_path = format!("/tmp/{}", uuid::Uuid::new_v4());
/// let dir = new_object_store_directory(store, &base_path, Some(0), 1, None, None).unwrap();
///
/// let mut schema_builder = Schema::builder();
/// let id_field = schema_builder.add_text_field("id", STRING);
/// let text_field = schema_builder.add_text_field("text", TEXT | STORED);
/// let schema = schema_builder.build();
///
/// let idx = tantivy::Index::create(dir, schema.clone(), IndexSettings::default()).unwrap();
///
///
pub fn new_object_store_directory(
    store: Arc<dyn ObjectStore>,
    base_path: &str,
    read_version: Option<u64>,
    write_version: u64,
    cache_loc: Option<&str>,
    rt: Option<tokio::runtime::Runtime>,
) -> Result<Box<dyn Directory>, std::io::Error> {
    if let Some(read_version) = read_version {
        if read_version > write_version {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "read version cannot be greater than write version",
            ));
        }
    }

    let cache_loc = cache_loc
        .map(|s| Arc::new(Path::new(&s).to_owned()))
        .unwrap_or(Arc::new(tempfile::tempdir()?.path().to_owned()));

    Ok(Box::new(ObjectStoreDirectory {
        store,
        base_path: base_path.to_string(),
        read_version,
        write_version,
        local_fs: Arc::new(LocalFileSystem::new()),
        cache_loc,
        rt: Arc::new(
            rt.unwrap_or(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?,
            ),
        ),
        atomic_rw_lock: Arc::new(Mutex::new(())),
    }))
}

#[cfg(test)]
mod test {
    use log::info;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tantivy::{
        doc,
        schema::{Schema, STORED, STRING, TEXT},
        IndexSettings,
    };

    use crate::new_object_store_directory;

    #[test]
    fn test_full_workflow() {
        env_logger::init();

        let store = Arc::new(LocalFileSystem::new());

        let base_path = format!("/tmp/{}", uuid::Uuid::new_v4().hyphenated());

        let dir =
            new_object_store_directory(store.clone(), &base_path, None, 0, None, None).unwrap();

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STRING);
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();

        info!("Creating index");
        let idx = tantivy::Index::create(dir, schema.clone(), IndexSettings::default()).unwrap();

        info!("Creating writer");
        let mut writer = idx.writer(1024 * 1024 * 64).unwrap();
        info!("Write doc 1");
        writer
            .add_document(doc!(
                id_field => "1",
                text_field => "hello world"
            ))
            .unwrap();
        info!("Write doc 2");
        writer
            .add_document(doc!(
                id_field => "2",
                text_field => "Deus Ex"
            ))
            .unwrap();
        info!("COMMIT!");
        writer.commit().unwrap();

        std::mem::drop(writer);
        std::mem::drop(idx);

        // try open again and add some data
        let dir =
            new_object_store_directory(store.clone(), &base_path, Some(0), 1, None, None).unwrap();

        info!("Open index");
        let idx = tantivy::Index::open(dir).unwrap();

        info!("Creating writer");
        let mut writer = idx.writer(1024 * 1024 * 64).unwrap();
        info!("Write doc 3");
        writer
            .add_document(doc!(
                id_field => "3",
                text_field => "bye bye"
            ))
            .unwrap();
        info!("COMMIT!");
        writer.commit().unwrap();
        info!("wait for merging threads");
        writer.wait_merging_threads().unwrap();

        std::mem::drop(idx);

        // open and search
        let dir =
            new_object_store_directory(store.clone(), &base_path, None, 0, None, None).unwrap();

        info!("Open index");
        let s3_idx = tantivy::Index::open(dir).unwrap();
        let query_parser =
            tantivy::query::QueryParser::for_index(&s3_idx, vec![id_field, text_field]);
        let searcher = s3_idx.reader().unwrap().searcher();

        info!("searching 1");
        let query = query_parser.parse_query("hello").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(top_docs.len(), 1);
        let doc = top_docs.get(0).unwrap().1;
        let retrieved_doc = searcher.doc(doc).unwrap();
        // we only store the text field, so there won't be an id field
        assert_eq!(
            retrieved_doc,
            doc!(
                text_field => "hello world"
            )
        );

        info!("searching 2");
        // No result -- not in this version
        let query = query_parser.parse_query("bye").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(top_docs.len(), 0);

        info!("searching 3");
        // finds the other doc
        let query = query_parser.parse_query("ex").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();

        assert_eq!(top_docs.len(), 1);
        let doc = top_docs.get(0).unwrap().1;
        let retrieved_doc = searcher.doc(doc).unwrap();
        // we only store the text field, so there won't be an id field
        assert_eq!(
            retrieved_doc,
            doc!(
                text_field => "Deus Ex"
            )
        );

        // open a differnet version and search
        let dir =
            new_object_store_directory(store.clone(), &base_path, None, 1, None, None).unwrap();
        info!("Open Index");
        let s3_idx = tantivy::Index::open(dir).unwrap();
        let searcher = s3_idx.reader().unwrap().searcher();

        info!("searching 4");
        // is in the newer version
        let query = query_parser.parse_query("bye").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(top_docs.len(), 1);
    }
}
