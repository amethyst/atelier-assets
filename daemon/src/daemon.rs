use std::{
    collections::HashMap,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    thread::JoinHandle,
};

use asset_hub::AssetHub;
use asset_hub_service::AssetHubService;
use distill_importer::{BoxedImporter, ImporterContext};
use distill_schema::data;
use file_asset_source::FileAssetSource;
use tokio::sync::oneshot::{self, Receiver, Sender};

use crate::{
    artifact_cache::ArtifactCache, asset_hub, asset_hub_service, capnp_db::Environment,
    error::Result, file_asset_source, file_tracker::FileTracker,
};

#[derive(Default)]
pub struct ImporterMap(HashMap<String, Box<dyn BoxedImporter>>);

impl ImporterMap {
    pub fn insert(&mut self, ext: &str, importer: Box<dyn BoxedImporter>) {
        self.0.insert(ext.to_lowercase(), importer);
    }

    pub fn get_by_path<'a>(&'a self, path: &Path) -> Option<&'a dyn BoxedImporter> {
        let lower_extension = path
            .extension()
            .map(|s| s.to_str().unwrap().to_lowercase())
            .unwrap_or_else(|| "".to_string());
        self.0.get(lower_extension.as_str()).map(|i| i.as_ref())
    }
}

struct AssetDaemonTables {
    /// Contains metadata about the daemon version and settings
    /// String -> Blob
    daemon_info: lmdb::Database,
}
impl AssetDaemonTables {
    fn new(db: &Environment) -> Result<Self> {
        Ok(Self {
            daemon_info: db.create_db(Some("daemon_info"), lmdb::DatabaseFlags::default())?,
        })
    }
}

const DAEMON_VERSION: u32 = 2;
pub struct AssetDaemon {
    pub db_dir: PathBuf,
    pub address: SocketAddr,
    pub importers: ImporterMap,
    pub importer_contexts: Vec<Box<dyn ImporterContext>>,
    pub asset_dirs: Vec<PathBuf>,
    pub clear_db_on_start: bool,
}

pub fn default_importer_contexts() -> Vec<Box<dyn ImporterContext + 'static>> {
    vec![distill_loader::if_handle_enabled!(Box::new(
        distill_loader::handle::HandleSerdeContextProvider
    ))]
}

#[allow(unused_mut)]
pub fn default_importers() -> Vec<(&'static str, Box<dyn BoxedImporter>)> {
    let mut importers: Vec<(&'static str, Box<dyn BoxedImporter>)> = vec![];

    distill_importer::if_serde_importers!(
        importers.push(("ron", Box::new(distill_importer::RonImporter::default())))
    );
    importers
}
impl Default for AssetDaemon {
    fn default() -> Self {
        let mut importer_map = ImporterMap::default();
        for (ext, importer) in default_importers() {
            importer_map.insert(ext, importer);
        }
        Self {
            db_dir: PathBuf::from(".assets_db"),
            address: "127.0.0.1:9999".parse().unwrap(),
            importers: importer_map,
            importer_contexts: default_importer_contexts(),
            asset_dirs: vec![PathBuf::from("assets")],
            clear_db_on_start: false,
        }
    }
}

impl AssetDaemon {
    pub fn with_db_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.db_dir = path.as_ref().to_owned();
        self
    }

    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = address;
        self
    }

    pub fn with_importer<B>(mut self, ext: &str, importer: B) -> Self
    where
        B: BoxedImporter + 'static,
    {
        self.importers.insert(ext, Box::new(importer));
        self
    }

    pub fn add_importer<B>(&mut self, ext: &str, importer: B)
    where
        B: BoxedImporter + 'static,
    {
        self.importers.insert(ext, Box::new(importer));
    }

    pub fn with_importers<B, I>(self, importers: I) -> Self
    where
        B: BoxedImporter + 'static,
        I: IntoIterator<Item = (&'static str, B)>,
    {
        importers.into_iter().fold(self, |this, (ext, importer)| {
            this.with_importer(ext, importer)
        })
    }

    pub fn with_importers_boxed<I>(mut self, importers: I) -> Self
    where
        I: IntoIterator<Item = (&'static str, Box<dyn BoxedImporter + 'static>)>,
    {
        for (ext, importer) in importers.into_iter() {
            self.importers.insert(ext, importer)
        }
        self
    }

    pub fn add_importers<B, I>(&mut self, importers: I)
    where
        B: BoxedImporter + 'static,
        I: IntoIterator<Item = (&'static str, B)>,
    {
        for (ext, importer) in importers {
            self.add_importer(ext, importer)
        }
    }

    pub fn with_importer_context(mut self, context: Box<dyn ImporterContext>) -> Self {
        self.importer_contexts.push(context);
        self
    }

    pub fn with_importer_contexts<I>(mut self, contexts: I) -> Self
    where
        I: IntoIterator<Item = Box<dyn ImporterContext>>,
    {
        self.importer_contexts.extend(contexts);
        self
    }

    pub fn with_asset_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.asset_dirs = dirs;
        self
    }

    /// Force the daemon to clean the cache on start
    pub fn with_clear_db_on_start(mut self) -> Self {
        self.clear_db_on_start = true;
        self
    }

    pub fn run(self) -> (JoinHandle<()>, Sender<bool>) {
        let (tx, rx) = oneshot::channel();

        let handle = thread::spawn(|| {
            let rpc_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let local = tokio::task::LocalSet::new();

            rpc_runtime.block_on(local.run_until(self.run_rpc_runtime(rx)))
        });

        (handle, tx)
    }

    async fn run_rpc_runtime(self, mut rx: Receiver<bool>) {
        let cache_dir = self.db_dir.join("cache");
        let _ = fs::create_dir(&self.db_dir);
        let _ = fs::create_dir(&cache_dir);

        for dir in self.asset_dirs.iter() {
            let _ = fs::create_dir_all(dir);
        }

        let asset_db = match Environment::new(&self.db_dir) {
            Ok(db) => db,
            Err(crate::Error::Lmdb(lmdb::Error::Other(1455))) => {
                Environment::with_map_size(&self.db_dir, 1 << 31)
                    .expect("failed to create asset db")
            }
            Err(err) => panic!("failed to create asset db: {:?}", err),
        };
        let asset_db = Arc::new(asset_db);

        try_clear_db(&asset_db, self.clear_db_on_start)
            .await
            .expect("failed to clear asset db");
        set_db_version(&asset_db)
            .await
            .expect("failed to check daemon version in asset db");

        let to_watch = self.asset_dirs.iter().map(|p| p.to_str().unwrap());
        let tracker = FileTracker::new(asset_db.clone(), to_watch);
        let tracker = Arc::new(tracker);

        let hub = AssetHub::new(asset_db.clone()).expect("failed to create asset hub");
        let hub = Arc::new(hub);

        let importers = Arc::new(self.importers);
        let ctxs = Arc::new(self.importer_contexts);
        let cache_db = match Environment::new(&cache_dir) {
            Ok(db) => db,
            Err(crate::Error::Lmdb(lmdb::Error::Other(1455))) => {
                Environment::with_map_size(&cache_dir, 1 << 31).expect("failed to create cache db")
            }
            Err(err) => panic!("failed to create cache db: {:?}", err),
        };
        let cache_db = Arc::new(cache_db);
        try_clear_db(&cache_db, self.clear_db_on_start)
            .await
            .expect("failed to clear cache db");
        let artifact_cache =
            ArtifactCache::new(&cache_db).expect("failed to create artifact cache");
        let artifact_cache = Arc::new(artifact_cache);

        let asset_source =
            FileAssetSource::new(&tracker, &hub, &asset_db, &importers, &artifact_cache, ctxs)
                .expect("failed to create asset source");

        let asset_source = Arc::new(asset_source);

        let service = AssetHubService::new(
            asset_db.clone(),
            hub.clone(),
            asset_source.clone(),
            tracker.clone(),
            artifact_cache.clone(),
        );
        let service = Arc::new(service);

        let addr = self.address;

        let shutdown_tracker = tracker.clone();

        let mut service_handle = tokio::task::spawn_local(async move { service.run(addr).await });
        let mut tracker_handle = tokio::task::spawn_local(async move { tracker.run().await });
        let mut asset_source_handle =
            tokio::task::spawn_local(async move { asset_source.run().await });

        log::info!("Starting Daemon Loop");
        loop {
            tokio::select! {
                done = &mut service_handle => done.expect("ServiceHandle panicked"),
                done = &mut tracker_handle => done.expect("FileTracker panicked"),
                done = &mut asset_source_handle => done.expect("AssetSource panicked"),
                done = &mut rx => match done {
                    Ok(_) => {
                        log::warn!("Shutting Down!");
                        shutdown_tracker.stop().await;
                        // shutdown_service.stop().await;
                        // shutdown_asset_source.stop().await;
                        // any value on this channel means shutdown
                        // TODO: better shutdown
                        return;
                    }
                    Err(_) => continue,
                }
            }
        }
    }
}

#[allow(clippy::string_lit_as_bytes)]
async fn try_clear_db(env: &Environment, force_clear_db: bool) -> Result<()> {
    use crate::capnp_db::DBTransaction;
    let tables = AssetDaemonTables::new(env).expect("failed to create AssetDaemon tables");
    let txn = env.ro_txn().await?;
    let info_key = "daemon_info".as_bytes();

    let mut clear_db = true;
    let daemon_info = txn.get::<data::daemon_info::Owned, &[u8]>(tables.daemon_info, &info_key)?;
    if let Some(info) = daemon_info {
        let info = info.get()?;
        if info.get_version() == DAEMON_VERSION {
            clear_db = false;
        }
    }

    if clear_db || force_clear_db {
        let unnamed_db = env
            .create_db(None, lmdb::DatabaseFlags::default())
            .expect("failed to open unnamed DB when checking daemon info");
        use lmdb::Cursor;
        let mut databases = Vec::new();
        for iter_result in txn
            .open_ro_cursor(unnamed_db)
            .expect("failed to create cursor when checking daemon info")
            .iter_start()
        {
            let (key, _) = iter_result
                .expect("failed to start iteration for cursor when checking daemon info");
            let db_name = std::str::from_utf8(key).expect("failed to parse db name");
            databases.push(
                env.create_db(Some(db_name), lmdb::DatabaseFlags::default())
                    .unwrap_or_else(|err| {
                        panic!("failed to open db with name {}: {}", db_name, err)
                    }),
            );
        }
        let mut txn = env.rw_txn().await?;
        for db in databases {
            txn.clear_db(db).expect("failed to clear db");
        }
        txn.commit()?;
    }
    Ok(())
}

#[allow(clippy::string_lit_as_bytes)]
async fn set_db_version(env: &Environment) -> Result<()> {
    let info_key = "daemon_info".as_bytes();
    let tables = AssetDaemonTables::new(env).expect("failed to create AssetDaemon tables");
    let mut txn = env.rw_txn().await?;
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<data::daemon_info::Builder<'_>>();
        m.set_version(DAEMON_VERSION);
    }
    txn.put(tables.daemon_info, &info_key, &value_builder)?;
    txn.commit()?;

    Ok(())
}
