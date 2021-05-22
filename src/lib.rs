pub use distill_core as core;
use distill_core::{AssetRef, AssetUuid};
pub use distill_daemon as daemon;
pub use distill_importer as importer;
pub use distill_loader as loader;
use distill_loader::handle::{Handle, SerdeContext};

pub fn make_handle<T>(uuid: AssetUuid) -> Handle<T> {
    SerdeContext::with_active(|loader_info_provider, ref_op_sender| {
        let load_handle = loader_info_provider
            .get_load_handle(&AssetRef::Uuid(uuid))
            .unwrap();
        Handle::<T>::new(ref_op_sender.clone(), load_handle)
    })
}

pub fn make_handle_from_str<T>(uuid_str: &str) -> Result<Handle<T>, distill_core::uuid::Error> {
    use std::str::FromStr;
    Ok(make_handle(AssetUuid(
        *distill_core::uuid::Uuid::from_str(uuid_str)?.as_bytes(),
    )))
}

#[cfg(feature = "type_uuid")]
#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        iter::FromIterator,
        path::PathBuf,
        str::FromStr,
        string::FromUtf8Error,
        sync::{Once, RwLock},
    };

    use distill_core::{type_uuid, type_uuid::TypeUuid, AssetRef, AssetTypeId, AssetUuid};
    use distill_daemon::{init_logging, AssetDaemon};
    use distill_importer::{
        AsyncImporter, ImportOp, ImportedAsset, ImporterValue, Result as ImportResult,
    };
    use distill_loader::{
        rpc_io::RpcIO,
        storage::{
            AssetLoadOp, AssetStorage, DefaultIndirectionResolver, LoadStatus, LoaderInfoProvider,
        },
        LoadHandle, Loader,
    };
    use futures::{future::BoxFuture, io::AsyncReadExt, AsyncRead};
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use uuid::Uuid;

    #[derive(Debug)]
    struct LoadState {
        size: Option<usize>,
        commit_version: Option<u32>,
        load_version: Option<u32>,
    }
    struct Storage {
        map: RwLock<HashMap<LoadHandle, LoadState>>,
    }
    impl AssetStorage for Storage {
        fn update_asset(
            &self,
            _loader_info: &dyn LoaderInfoProvider,
            _asset_type: &AssetTypeId,
            data: Vec<u8>,
            loader_handle: LoadHandle,
            load_op: AssetLoadOp,
            version: u32,
        ) -> distill_loader::Result<()> {
            println!("update asset {:?} data size {}", loader_handle, data.len());
            let mut map = self.map.write().unwrap();
            let state = map.entry(loader_handle).or_insert(LoadState {
                size: None,
                commit_version: None,
                load_version: None,
            });

            state.size = Some(data.len());
            state.load_version = Some(version);
            load_op.complete();
            Ok(())
        }

        fn commit_asset_version(
            &self,
            _asset_type: &AssetTypeId,
            loader_handle: LoadHandle,
            version: u32,
        ) {
            println!("commit asset {:?}", loader_handle,);
            let mut map = self.map.write().unwrap();
            let state = map.get_mut(&loader_handle).unwrap();

            assert!(state.load_version.unwrap() == version);
            state.commit_version = Some(version);
            state.load_version = None;
        }

        fn free(&self, _asset_type: &AssetTypeId, loader_handle: LoadHandle, _version: u32) {
            println!("free asset {:?}", loader_handle);
            self.map.write().unwrap().remove(&loader_handle);
        }
    }

    /// Removes file comments (begin with `#`) and empty lines.
    #[derive(Clone, Debug, Default, Deserialize, Serialize, TypeUuid)]
    #[uuid = "346e6a3e-3278-4c53-b21c-99b4350662db"]
    pub struct TxtFormat;
    impl TxtFormat {
        fn from_utf8(&self, vec: Vec<u8>) -> std::result::Result<String, FromUtf8Error> {
            String::from_utf8(vec).map(|data| {
                let processed = data
                    .lines()
                    .map(|line| {
                        line.find('#')
                            .map(|index| line.split_at(index).0)
                            .unwrap_or(line)
                            .trim()
                    })
                    .filter(|line| !line.is_empty())
                    .flat_map(|line| line.chars().chain(std::iter::once('\n')));
                String::from_iter(processed)
            })
        }
    }
    /// A simple state for Importer to retain the same UUID between imports
    /// for all single-asset source files
    #[derive(Default, Deserialize, Serialize, TypeUuid)]
    #[uuid = "c50c36fe-8df0-48fe-b1d7-3e69ab00a997"]
    pub struct TxtImporterState {
        id: Option<AssetUuid>,
    }
    #[derive(TypeUuid)]
    #[uuid = "fa50e08c-af6c-4ada-aed1-447c116d63bc"]
    struct TxtImporter;
    impl AsyncImporter for TxtImporter {
        type Options = TxtFormat;
        type State = TxtImporterState;

        fn version_static() -> u32
        where
            Self: Sized,
        {
            1
        }

        fn version(&self) -> u32 {
            Self::version_static()
        }

        fn import<'a>(
            &'a self,
            _op: &'a mut ImportOp,
            source: &'a mut (dyn AsyncRead + Unpin + Send + Sync),
            txt_format: &'a Self::Options,
            state: &'a mut Self::State,
        ) -> BoxFuture<'a, ImportResult<ImporterValue>> {
            Box::pin(async move {
                if state.id.is_none() {
                    state.id = Some(AssetUuid(*uuid::Uuid::new_v4().as_bytes()));
                }
                let mut bytes = Vec::new();
                source.read_to_end(&mut bytes).await?;
                let parsed_asset_data = txt_format
                    .from_utf8(bytes)
                    .expect("Failed to construct string asset.");

                let load_deps = parsed_asset_data
                    .lines()
                    .filter_map(|line| Uuid::from_str(line).ok())
                    .map(|uuid| AssetRef::Uuid(AssetUuid(*uuid.as_bytes())))
                    .collect::<Vec<AssetRef>>();

                Ok(ImporterValue {
                    assets: vec![ImportedAsset {
                        id: state.id.expect("AssetUuid not generated"),
                        search_tags: Vec::new(),
                        build_deps: Vec::new(),
                        load_deps,
                        asset_data: Box::new(parsed_asset_data),
                        build_pipeline: None,
                    }],
                })
            })
        }
    }

    fn wait_for_status(
        status: LoadStatus,
        handle: LoadHandle,
        loader: &mut Loader,
        storage: &Storage,
    ) -> bool {
        for _ in 0..100 {
            if std::mem::discriminant(&status)
                == std::mem::discriminant(&loader.get_load_status(handle))
            {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(100)); // waiting for daemon before we try again
            if let Err(e) = loader.process(storage, &DefaultIndirectionResolver) {
                println!("err {:?}", e);
            }
            println!("tick (100ms)");
        }

        unreachable!("Never got to desired status.")
    }

    static INIT: Once = Once::new();

    #[test]
    #[serial]
    fn test_connect() {
        INIT.call_once(|| {
            init_logging().unwrap();
        });

        // Start daemon in a separate thread
        let daemon_port = 2500;
        let daemon_address = format!("127.0.0.1:{}", daemon_port);

        let (daemon_handle, mut tx) = spawn_daemon(&daemon_address);

        let mut loader = Loader::new(Box::new(RpcIO::new(daemon_address).unwrap()));
        let handle = loader.add_ref(
            // asset uuid of "tests/assets/asset.txt"
            "b24d209d-6622-4d78-a983-731e8b76f04d",
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        assert!(wait_for_status(
            LoadStatus::Loaded,
            handle,
            &mut loader,
            &storage
        ));
        loader.remove_ref(handle);
        assert!(wait_for_status(
            LoadStatus::NotRequested,
            handle,
            &mut loader,
            &storage
        ));

        tx.send(true).unwrap();
        daemon_handle.join().unwrap();
    }

    #[test]
    #[serial]
    fn test_load_with_dependencies() {
        INIT.call_once(|| {
            init_logging().unwrap();
        });

        // Start daemon in a separate thread
        let daemon_port = 2505;
        let daemon_address = format!("127.0.0.1:{}", daemon_port);

        let (daemon_handle, mut tx) = spawn_daemon(&daemon_address);

        let mut loader = Loader::new(Box::new(RpcIO::new(daemon_address).unwrap()));
        let handle = loader.add_ref(
            // asset uuid of "tests/assets/asset_a.txt"
            "d83bb247-2710-4c10-83df-d7daa53e19bf",
        );
        let storage = &mut Storage {
            map: RwLock::new(HashMap::new()),
        };
        wait_for_status(LoadStatus::Loaded, handle, &mut loader, &storage);

        // Check that dependent assets are loaded
        let asset_handles = asset_tree()
            .iter()
            .map(|(asset_uuid, file_name)| {
                let asset_load_handle = loader
                    .get_load(*asset_uuid)
                    .unwrap_or_else(|| panic!("Expected `{}` to be loaded.", file_name));

                (asset_load_handle, *file_name)
            })
            .collect::<Vec<(LoadHandle, &'static str)>>();

        asset_handles
            .iter()
            .for_each(|(asset_load_handle, file_name)| {
                assert_eq!(
                    std::mem::discriminant(&LoadStatus::Loaded),
                    std::mem::discriminant(&loader.get_load_status(*asset_load_handle)),
                    "Expected `{}` to be loaded.",
                    file_name
                );
            });

        // Remove reference to top level asset.
        loader.remove_ref(handle);
        wait_for_status(LoadStatus::NotRequested, handle, &mut loader, &storage);

        // Remove ref when unloading top level asset.
        asset_handles
            .iter()
            .for_each(|(asset_load_handle, file_name)| {
                println!("Waiting for {} to be `NotRequested`.", file_name);
                wait_for_status(
                    LoadStatus::NotRequested,
                    *asset_load_handle,
                    &mut loader,
                    &storage,
                );
            });

        tx.send(true).unwrap();
        daemon_handle.join().unwrap();
    }

    fn asset_tree() -> Vec<(AssetUuid, &'static str)> {
        [
            ("d83bb247-2710-4c10-83df-d7daa53e19bf", "asset_a.txt"),
            ("23da999a-a974-4d0e-918a-f226ea0b3e69", "asset_b.txt"),
            ("40becaa7-cedb-466a-afee-41fecb1c916f", "asset_c.txt"),
            ("14f807b9-69ef-484b-9cb8-44787883b86d", "asset_d.txt"),
        ]
        .iter()
        .map(|(id, file_name)| {
            let asset_uuid = *uuid::Uuid::parse_str(id)
                .unwrap_or_else(|_| panic!("Failed to parse `{}` as `Uuid`.", id))
                .as_bytes();

            (AssetUuid(asset_uuid), *file_name)
        })
        .collect::<Vec<(AssetUuid, &'static str)>>()
    }

    fn spawn_daemon(
        daemon_address: &str,
    ) -> (
        std::thread::JoinHandle<()>,
        async_oneshot::Sender<bool>,
    ) {
        let daemon_address = daemon_address
            .parse()
            .expect("Failed to parse string as `SocketAddr`.");
        let tests_path = PathBuf::from_iter(&[env!("CARGO_MANIFEST_DIR"), "tests"]);

        AssetDaemon::default()
            .with_db_path(tests_path.join("assets_db"))
            .with_address(daemon_address)
            .with_importer("txt", TxtImporter)
            .with_asset_dirs(vec![tests_path.join("assets")])
            .run()
    }
}
