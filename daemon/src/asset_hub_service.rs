use std::{
    collections::{HashMap, HashSet},
    path,
    rc::Rc,
    sync::Arc,
};

use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use distill_core::utils::{self, canonicalize_path};
use distill_importer::SerializedAsset;
use distill_schema::{
    build_artifact_metadata,
    data::{
        artifact, asset_change_log_entry,
        asset_metadata::{self, latest_artifact},
        AssetSource,
    },
    parse_artifact_metadata, parse_db_asset_ref,
    service::asset_hub,
};
use futures::{AsyncReadExt, TryFutureExt};

use crate::{
    artifact_cache::ArtifactCache,
    asset_hub::{AssetBatchEvent, AssetHub},
    capnp_db::{CapnpCursor as _, Environment, RoTransaction},
    error::Error,
    file_asset_source::FileAssetSource,
    file_tracker::FileTracker,
};

// crate::Error has `impl From<crate::Error> for capnp::Error`
type Promise<T> = capnp::capability::Promise<T, capnp::Error>;
type Result<T> = std::result::Result<T, Error>;

struct ServiceContext {
    hub: Arc<AssetHub>,
    file_source: Arc<FileAssetSource>,
    file_tracker: Arc<FileTracker>,
    artifact_cache: Arc<ArtifactCache>,
    db: Arc<Environment>,
}

pub(crate) struct AssetHubService {
    ctx: Arc<ServiceContext>,
}

struct SnapshotTxn {
    ctx: Arc<ServiceContext>,
    // txn is owned by service context, so it's lifetime is bound to this object's lifetime
    txn: RoTransaction<'static>,
}

impl SnapshotTxn {
    async fn new(ctx: Arc<ServiceContext>) -> Self {
        let txn = ctx.db.ro_txn().await.unwrap();
        // The transaction can live at least as long as ServiceContext, which this object holds onto.
        // It is only ever borrowed for a lifetime bound to the self reference.
        let txn = unsafe { std::mem::transmute::<RoTransaction<'_>, RoTransaction<'static>>(txn) };
        Self { ctx, txn }
    }

    fn txn(&self) -> &RoTransaction<'_> {
        &self.txn
    }

    fn ctx(&self) -> &Arc<ServiceContext> {
        &self.ctx
    }
}

// RPC interface implementations

struct AssetHubSnapshotImpl {
    txn: Arc<SnapshotTxn>,
}

struct AssetHubImpl {
    ctx: Arc<ServiceContext>,
    local: Rc<async_executor::LocalExecutor<'static>>
}

fn build_artifact_message<T: AsRef<[u8]>>(
    artifact: &SerializedAsset<T>,
) -> capnp::message::Builder<capnp::message::HeapAllocator> {
    let mut value_builder = capnp::message::Builder::new_default();
    {
        let mut m = value_builder.init_root::<artifact::Builder<'_>>();
        let mut metadata = m.reborrow().init_metadata();
        build_artifact_metadata(&artifact.metadata, &mut metadata);
        let slice: &[u8] = artifact.data.as_ref();
        m.reborrow().set_data(slice);
    }
    value_builder
}

fn artifact_to_serialized_asset<'a>(
    artifact: &artifact::Reader<'a>,
) -> Result<SerializedAsset<&'a [u8]>> {
    let metadata = parse_artifact_metadata(&artifact.get_metadata()?);
    Ok(SerializedAsset {
        metadata,
        data: artifact.get_data()?,
    })
}

impl AssetHubSnapshotImpl {
    async fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            txn: Arc::new(SnapshotTxn::new(ctx).await),
        }
    }

    fn get_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAssetMetadataResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut metadatas = Vec::new();
        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?).ok_or(Error::UuidLength)?;
            let value = ctx.hub.get_metadata(txn, &id);
            if let Some(metadata) = value {
                metadatas.push(metadata);
            }
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }

    fn get_asset_metadata_with_dependencies(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataWithDependenciesParams,
        mut results: asset_hub::snapshot::GetAssetMetadataWithDependenciesResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut metadatas = HashMap::new();
        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?).ok_or(Error::UuidLength)?;
            let value = ctx.hub.get_metadata(txn, &id);
            if let Some(metadata) = value {
                metadatas.insert(id, metadata);
            }
        }
        let mut missing_metadata = HashSet::new();
        for metadata in metadatas.values() {
            if let latest_artifact::Artifact(Ok(artifact)) =
                metadata.get()?.get_latest_artifact().which()?
            {
                for dep in artifact.get_load_deps()? {
                    let dep = *parse_db_asset_ref(&dep).expect_uuid();
                    if !metadatas.contains_key(&dep) {
                        missing_metadata.insert(dep);
                    }
                }
            }
        }
        for id in missing_metadata {
            let value = ctx.hub.get_metadata(txn, &id);
            if let Some(metadata) = value {
                metadatas.insert(id, metadata);
            }
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.values().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }

    fn get_all_asset_metadata(
        &mut self,
        _params: asset_hub::snapshot::GetAllAssetMetadataParams,
        mut results: asset_hub::snapshot::GetAllAssetMetadataResults,
    ) -> Result<()> {
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut metadatas = Vec::new();
        for (_, value) in ctx.hub.get_metadata_iter(txn)?.capnp_iter_start() {
            let value = value?;
            let metadata = value.into_typed::<asset_metadata::Owned>();
            metadatas.push(metadata);
        }
        let mut results_builder = results.get();
        let assets = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, metadata) in metadatas.iter().enumerate() {
            let metadata = metadata.get()?;
            assets.set_with_caveats(idx as u32, metadata)?;
        }
        Ok(())
    }

    async fn get_import_artifacts(
        snapshot: Arc<SnapshotTxn>,
        params: asset_hub::snapshot::GetImportArtifactsParams,
        mut results: asset_hub::snapshot::GetImportArtifactsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = snapshot.ctx();
        let txn = snapshot.txn();
        let mut regen_artifacts = Vec::new();
        let mut cached_artifacts = Vec::new();
        let mut scratch_buf = Vec::new();
        let cache_txn = ctx.artifact_cache.ro_txn().await?;

        let request_uuid = uuid::Uuid::new_v4();
        log::trace!(
            "get_import_artifacts Gathering artifacts {:?}",
            request_uuid
        );

        for id in params.get_assets()? {
            let id = utils::uuid_from_slice(id.get_id()?).ok_or(Error::UuidLength)?;
            log::trace!("{:?} get_import_artifacts for id {:?}", request_uuid, id);
            let value = ctx.hub.get_metadata(txn, &id);
            if let Some(metadata) = value {
                log::trace!("metadata available for id {:?}", id);

                // retreive artifact data from cache if available
                let mut need_regen = true;
                if let latest_artifact::Artifact(Ok(artifact)) =
                    metadata.get()?.get_latest_artifact().which()?
                {
                    let hash = u64::from_le_bytes(utils::make_array(artifact.get_hash()?));
                    if let Some(artifact) = ctx.artifact_cache.get(&cache_txn, hash).await {
                        cached_artifacts.push(artifact);
                        need_regen = false;
                    } else {
                        log::trace!("cache miss for asset {:?} with hash {:?}", id, hash);
                    }
                }

                if need_regen {
                    let metadata = metadata.get()?;
                    match metadata.get_source()? {
                        AssetSource::File => {
                            log::trace!("regenerating import artifact from file for {:?}", id);
                            let (_, artifact) = ctx
                                .file_source
                                .regenerate_import_artifact(txn, &id, &mut scratch_buf)
                                .await?;
                            log::trace!("finished regenerating import artifact for {:?}", id);
                            let capnp_artifact = build_artifact_message(&artifact);
                            log::trace!("built artifact message for {:?}", id);
                            regen_artifacts.push(capnp_artifact);
                        }
                    }
                } else {
                    log::trace!("using cached import artifact for {:?}", id);
                }
            } else {
                log::trace!("metadata not available for id {:?}", id);
            }
        }

        log::trace!("get_import_artifacts Building Response {:?}", request_uuid);
        let mut results_builder = results.get();
        let mut artifact_results = results_builder
            .reborrow()
            .init_artifacts(cached_artifacts.len() as u32 + regen_artifacts.len() as u32);
        for (idx, artifact) in cached_artifacts.iter().enumerate() {
            artifact_results
                .reborrow()
                .set_with_caveats(idx as u32, artifact.get()?)?;
        }
        for (idx, artifact) in regen_artifacts.iter().enumerate() {
            artifact_results.reborrow().set_with_caveats(
                idx as u32,
                artifact.get_root_as_reader::<artifact::Reader<'_>>()?,
            )?;
        }
        Ok(())
    }

    fn get_latest_asset_change(
        &mut self,
        _params: asset_hub::snapshot::GetLatestAssetChangeParams,
        mut results: asset_hub::snapshot::GetLatestAssetChangeResults,
    ) -> Result<()> {
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let change_num = ctx.hub.get_latest_asset_change(txn)?;
        results.get().set_num(change_num);
        Ok(())
    }

    fn get_asset_changes(
        &mut self,
        params: asset_hub::snapshot::GetAssetChangesParams,
        mut results: asset_hub::snapshot::GetAssetChangesResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut changes = Vec::new();
        let iter = ctx.hub.get_asset_changes_iter(txn)?;
        let iter = iter.capnp_iter_from(&params.get_start().to_le_bytes());
        let mut count = params.get_count() as usize;
        if count == 0 {
            count = std::usize::MAX;
        }
        for (_, value) in iter.take(count) {
            let value = value?;
            let change = value.into_typed::<asset_change_log_entry::Owned>();
            changes.push(change);
        }
        let mut results_builder = results.get();
        let changes_results = results_builder
            .reborrow()
            .init_changes(changes.len() as u32);
        for (idx, change) in changes.iter().enumerate() {
            let change = change.get()?;
            changes_results.set_with_caveats(idx as u32, change)?;
        }
        Ok(())
    }

    fn get_path_for_assets(
        &mut self,
        params: asset_hub::snapshot::GetPathForAssetsParams,
        mut results: asset_hub::snapshot::GetPathForAssetsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut asset_paths = Vec::new();
        for id in params.get_assets()? {
            let asset_uuid = utils::uuid_from_slice(id.get_id()?).ok_or(Error::UuidLength)?;
            let path = ctx.file_source.get_asset_path(txn, &asset_uuid);
            if let Some(path) = path {
                for dir in ctx.file_tracker.get_watch_dirs() {
                    let canonicalized_dir = canonicalize_path(&dir);
                    if path.starts_with(&canonicalized_dir) {
                        let relative_path = path
                            .strip_prefix(canonicalized_dir)
                            .expect("error stripping prefix")
                            .to_path_buf();
                        let relative_path = canonicalize_path(&relative_path)
                            .to_string_lossy()
                            .replace("\\", "/");
                        asset_paths.push((id, relative_path));
                    }
                }
            }
        }
        let mut results_builder = results.get();
        let mut assets = results_builder
            .reborrow()
            .init_paths(asset_paths.len() as u32);
        for (idx, (asset, path)) in asset_paths.iter().enumerate() {
            assets.reborrow().get(idx as u32).set_path(path.as_bytes());
            assets
                .reborrow()
                .get(idx as u32)
                .init_id()
                .set_id(asset.get_id()?);
        }
        Ok(())
    }

    fn get_assets_for_paths(
        &mut self,
        params: asset_hub::snapshot::GetAssetsForPathsParams,
        mut results: asset_hub::snapshot::GetAssetsForPathsResults,
    ) -> Result<()> {
        let params = params.get()?;
        let ctx = self.txn.ctx();
        let txn = self.txn.txn();
        let mut metadatas = Vec::new();
        for request_path in params.get_paths()? {
            let request_path = request_path?;
            let path_str = std::str::from_utf8(request_path)?.to_string();
            let path = path::PathBuf::from(path_str);
            let mut metadata = None;
            if path.is_relative() {
                for dir in ctx.file_tracker.get_watch_dirs() {
                    let canonicalized = canonicalize_path(&dir.join(&path));
                    metadata = ctx.file_source.get_metadata(txn, &canonicalized);
                    if metadata.is_some() {
                        break;
                    }
                }
            } else {
                let canonicalized = canonicalize_path(&path);
                metadata = ctx.file_source.get_metadata(txn, &canonicalized)
            }
            if let Some(metadata) = metadata {
                metadatas.push((request_path, metadata));
            }
        }
        let mut results_builder = results.get();
        let mut results = results_builder
            .reborrow()
            .init_assets(metadatas.len() as u32);
        for (idx, (path, assets)) in metadatas.iter().enumerate() {
            let assets = assets.get()?.get_assets()?;
            let num_assets = assets.len();
            let mut asset_results = results.reborrow().get(idx as u32).init_assets(num_assets);
            for (idx, asset) in assets.iter().enumerate() {
                asset_results
                    .reborrow()
                    .get(idx as u32)
                    .set_id(asset.get_id()?.get_id()?);
            }
            results.reborrow().get(idx as u32).set_path(path);
        }
        Ok(())
    }

    async fn update_asset(
        snapshot: Arc<SnapshotTxn>,
        params: asset_hub::snapshot::UpdateAssetParams,
        mut results: asset_hub::snapshot::UpdateAssetResults,
    ) -> Result<()> {
        let params = params.get()?;
        let txn = &snapshot.txn;
        let ctx = &snapshot.ctx;
        // TODO move the below parts into FileAssetSource
        let new_artifact = artifact_to_serialized_asset(&params.get_asset()?)?.to_vec();
        let asset_uuid = new_artifact.metadata.asset_id;
        let asset_metadata = ctx.hub.get_metadata(txn, &asset_uuid);
        let mut scratch_buf = Vec::new();
        if let Some(asset_metadata) = asset_metadata {
            match asset_metadata.get()?.get_source()? {
                AssetSource::File => {
                    let path = ctx.file_source.get_asset_path(txn, &asset_uuid);
                    if let Some(path) = path {
                        let source_metadata = ctx
                            .file_source
                            .get_metadata(txn, &path)
                            .expect("inconsistent source metadata");
                        let source_metadata = source_metadata.get()?;
                        let mut assets = vec![new_artifact];
                        for asset in source_metadata.get_assets()? {
                            let source_asset_id = utils::uuid_from_slice(asset.get_id()?.get_id()?)
                                .ok_or(Error::UuidLength)?;
                            if source_asset_id != asset_uuid {
                                // TODO maybe extract into a function, and use the cache in the future
                                let (_, artifact) = ctx
                                    .file_source
                                    .regenerate_import_artifact(
                                        txn,
                                        &source_asset_id,
                                        &mut scratch_buf,
                                    )
                                    .await?;
                                assets.push(artifact);
                            }
                        }
                        let export_results = ctx.file_source.export_source(path, assets).await?;
                        let updated_asset_metadata =
                            export_results.iter().find(|a| a.id == asset_uuid);
                        if let Some(metadata) = updated_asset_metadata {
                            if let Some(artifact) = &metadata.artifact {
                                let mut results_builder = results.get();
                                results_builder.set_new_import_hash(&artifact.id.0.to_le_bytes());
                                Ok(())
                            } else {
                                Err(Error::Custom(
                                    "Metadata for the updated asset does not contain artifact metadata after exporting"
                                        .into(),
                                ))
                            }
                        } else {
                            Err(Error::Custom(
                                "Metadata for the updated asset doesn't exist after exporting"
                                    .into(),
                            ))
                        }
                    } else {
                        Err(Error::Custom("Source file does not exist for asset".into()))
                    }
                }
            }
        } else {
            Err(Error::Custom(
                "Unable to find asset metadata for asset".into(),
            ))
        }
    }
}

#[allow(clippy::unit_arg)]
impl asset_hub::Server for AssetHubImpl {
    fn register_listener(
        &mut self,
        params: asset_hub::RegisterListenerParams,
        results: asset_hub::RegisterListenerResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::Server::register_listener");
        Promise::ok(pry!(AssetHubImpl::register_listener(self, params, results)))
    }

    fn get_snapshot(
        &mut self,
        params: asset_hub::GetSnapshotParams,
        results: asset_hub::GetSnapshotResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::Server::get_snapshot");
        let fut = AssetHubImpl::get_snapshot(self.ctx.clone(), params, results);
        Promise::from_future(async { fut.await.map_err(|e| e.into()) })
    }
}
impl AssetHubImpl {
    fn register_listener(
        &mut self,
        params: asset_hub::RegisterListenerParams,
        _results: asset_hub::RegisterListenerResults,
    ) -> Result<()> {
        let params = params.get()?;
        let listener = Rc::new(params.get_listener()?);
        let ctx = self.ctx.clone();
        let (tx, rx) = async_channel::bounded(16);
        tx.try_send(AssetBatchEvent::Commit).unwrap();

        let tx = self.ctx.hub.register_listener(tx);

        self.local.spawn(async move {
            while rx.recv().await.is_ok() {
                let mut request = listener.update_request();
                let snapshot = AssetHubSnapshotImpl::new(ctx.clone()).await;
                let latest_change = ctx
                    .hub
                    .get_latest_asset_change(snapshot.txn.txn())
                    .expect("failed to get latest change");
                request.get().set_latest_change(latest_change);
                request.get().set_snapshot(capnp_rpc::new_client(snapshot));
                if request.send().promise.await.is_err() {
                    ctx.hub.drop_listener(tx);
                    break;
                }
            }
        }).detach();

        Ok(())
    }

    async fn get_snapshot(
        ctx: Arc<ServiceContext>,
        _params: asset_hub::GetSnapshotParams,
        mut results: asset_hub::GetSnapshotResults,
    ) -> Result<()> {
        let snapshot = AssetHubSnapshotImpl::new(ctx).await;
        results.get().set_snapshot(capnp_rpc::new_client(snapshot));
        Ok(())
    }
}

impl AssetHubService {
    pub fn new(
        db: Arc<Environment>,
        hub: Arc<AssetHub>,
        file_source: Arc<FileAssetSource>,
        file_tracker: Arc<FileTracker>,
        artifact_cache: Arc<ArtifactCache>,
    ) -> AssetHubService {
        AssetHubService {
            ctx: Arc::new(ServiceContext {
                hub,
                file_source,
                file_tracker,
                artifact_cache,
                db,
            }),
        }
    }

    pub async fn run(&self, addr: std::net::SocketAddr) {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = async {
            let listener = async_net::TcpListener::bind(&addr).await?;

            loop {
                let (stream, _) = listener.accept().await?;
                let ctx = self.ctx.clone();

                std::thread::spawn(|| {
                    log::info!("async_net::TcpListener accepted");

                    stream.set_nodelay(true).unwrap();
                    let (reader, writer) = stream.split();

                    let local = Rc::new(async_executor::LocalExecutor::new());
                    let service_impl = AssetHubImpl {
                        ctx,
                        local: local.clone()
                    };

                    let hub_impl: asset_hub::Client = capnp_rpc::new_client(service_impl);

                    let network = twoparty::VatNetwork::new(
                        reader,
                        writer,
                        rpc_twoparty_capnp::Side::Server,
                        Default::default(),
                    );

                    let rpc_system = RpcSystem::new(Box::new(network), Some(hub_impl.client));
                    async_io::block_on(local.run(rpc_system.map_err(|_| ()))).unwrap();
                });
            }
        }
        .await;
        // NOTE(happens): This will only fail if we can't set the stream
        // parameters on startup, which is a cause for panic in any case.
        // NOTE(kabergstrom): It also seems to happen when the main thread
        // is aborted and this is run on a background thread
        result.expect("Failed to run tcp listener");
    }
}

#[allow(clippy::unit_arg)]
impl asset_hub::snapshot::Server for AssetHubSnapshotImpl {
    fn get_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataParams,
        results: asset_hub::snapshot::GetAssetMetadataResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_asset_metadata");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_asset_metadata(
            self, params, results
        )))
    }

    fn get_asset_metadata_with_dependencies(
        &mut self,
        params: asset_hub::snapshot::GetAssetMetadataWithDependenciesParams,
        results: asset_hub::snapshot::GetAssetMetadataWithDependenciesResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_asset_metadata_with_dependencies");
        Promise::ok(pry!(
            AssetHubSnapshotImpl::get_asset_metadata_with_dependencies(self, params, results)
        ))
    }

    fn get_all_asset_metadata(
        &mut self,
        params: asset_hub::snapshot::GetAllAssetMetadataParams,
        results: asset_hub::snapshot::GetAllAssetMetadataResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_all_asset_metadata");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_all_asset_metadata(
            self, params, results
        )))
    }

    fn get_import_artifacts(
        &mut self,
        params: asset_hub::snapshot::GetImportArtifactsParams,
        results: asset_hub::snapshot::GetImportArtifactsResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_import_artifacts");
        let fut = AssetHubSnapshotImpl::get_import_artifacts(self.txn.clone(), params, results);
        Promise::from_future(async { fut.await.map_err(|e| e.into()) })
    }

    fn get_latest_asset_change(
        &mut self,
        params: asset_hub::snapshot::GetLatestAssetChangeParams,
        results: asset_hub::snapshot::GetLatestAssetChangeResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_latest_asset_change");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_latest_asset_change(
            self, params, results
        )))
    }

    fn get_asset_changes(
        &mut self,
        params: asset_hub::snapshot::GetAssetChangesParams,
        results: asset_hub::snapshot::GetAssetChangesResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_asset_changes");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_asset_changes(
            self, params, results
        )))
    }

    fn get_path_for_assets(
        &mut self,
        params: asset_hub::snapshot::GetPathForAssetsParams,
        results: asset_hub::snapshot::GetPathForAssetsResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_path_for_assets");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_path_for_assets(
            self, params, results
        )))
    }

    fn get_assets_for_paths(
        &mut self,
        params: asset_hub::snapshot::GetAssetsForPathsParams,
        results: asset_hub::snapshot::GetAssetsForPathsResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::get_assets_for_paths");
        Promise::ok(pry!(AssetHubSnapshotImpl::get_assets_for_paths(
            self, params, results
        )))
    }

    fn update_asset(
        &mut self,
        params: asset_hub::snapshot::UpdateAssetParams,
        results: asset_hub::snapshot::UpdateAssetResults,
    ) -> Promise<()> {
        log::trace!("asset_hub::snapshot::Server::update_asset");
        let fut = AssetHubSnapshotImpl::update_asset(self.txn.clone(), params, results);
        Promise::from_future(async { fut.await.map_err(|e| e.into()) })
    }
}
