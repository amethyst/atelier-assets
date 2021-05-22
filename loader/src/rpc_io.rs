use std::{error::Error, path::PathBuf, sync::Mutex};

use capnp::message::ReaderOptions;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use crossbeam_channel::{unbounded, Receiver, Sender};
use distill_core::{utils, AssetMetadata, AssetUuid};
use distill_schema::{data::asset_change_event, parse_db_metadata, service::asset_hub};
use futures_util::AsyncReadExt;

use crate::{
    io::{DataRequest, LoaderIO, MetadataRequest, MetadataRequestResult, ResolveRequest},
    loader::LoaderState,
};
use std::rc::Rc;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;

/// a connection to the capnp provided rpc and an event receiver for SnapshotChange events
struct RpcConnection {
    snapshot: asset_hub::snapshot::Client,
    snapshot_rx: Receiver<SnapshotChange>,
}

/// an event which represents change to the assets
struct SnapshotChange {
    snapshot: asset_hub::snapshot::Client,
    changed_assets: Vec<AssetUuid>,
    deleted_assets: Vec<AssetUuid>,
    changed_paths: Vec<PathBuf>,
    deleted_paths: Vec<PathBuf>,
}

enum InternalConnectionState {
    None,
    Connecting(async_oneshot::Receiver<Result<RpcConnection, Box<dyn Error>>>),
    Connected(RpcConnection),
    Error(Box<dyn Error>),
}

pub mod error {
    use std::fmt;

    /// Error returned by the `try_recv` function on `Receiver`.
    #[derive(Debug, Eq, PartialEq)]
    pub enum TryRecvError {
        /// The send half of the channel has not yet sent a value.
        Empty,

        /// The send half of the channel was dropped without sending a value.
        Closed,
    }

    impl fmt::Display for TryRecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TryRecvError::Empty => write!(fmt, "channel empty"),
                TryRecvError::Closed => write!(fmt, "channel closed"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}
}

/// the executor and tasks, as well as the connection state
struct RpcRuntime {
    local: Rc<async_executor::LocalExecutor<'static>>,
    connection: InternalConnectionState,
}

// While capnp_rpc does not impl Send or Sync, in our usage of the API there can only be one thread
// accessing the internal state at any time due to Mutex. The !Send constraint in capnp_rpc is because
// of internal object lifetime management that is unsafe in the face of multiple threads accessing data
// from separate objects.
unsafe impl Send for RpcRuntime {}

impl RpcRuntime {
    fn check_asset_changes(&mut self, loader: &LoaderState) {
        self.connection =
            match std::mem::replace(&mut self.connection, InternalConnectionState::None) {
                InternalConnectionState::Connected(mut conn) => {
                    if let Ok(change) = conn.snapshot_rx.try_recv() {
                        log::trace!("RpcRuntime check_asset_changes Ok(change)");
                        conn.snapshot = change.snapshot;
                        let mut changed_assets = Vec::new();
                        for asset in change.changed_assets {
                            log::trace!(
                                "RpcRuntime check_asset_changes changed asset.id: {:?}",
                                asset
                            );
                            changed_assets.push(asset);
                        }
                        for asset in change.deleted_assets {
                            log::trace!(
                                "RpcRuntime check_asset_changes deleted asset.id: {:?}",
                                asset
                            );
                            changed_assets.push(asset);
                        }
                        loader.invalidate_assets(&changed_assets);
                        let mut changed_paths = Vec::new();
                        for path in change.changed_paths {
                            changed_paths.push(path);
                        }
                        for path in change.deleted_paths {
                            changed_paths.push(path);
                        }
                        loader.invalidate_paths(&changed_paths);
                    }
                    InternalConnectionState::Connected(conn)
                }
                c => c,
            };
    }

    fn connect(&mut self, connect_string: String) {
        match self.connection {
            InternalConnectionState::Connected(_) | InternalConnectionState::Connecting(_) => {
                panic!("Trying to connect while already connected or connecting")
            }
            _ => {}
        };

        let (mut conn_tx, conn_rx) = async_oneshot::oneshot();

        let local = self.local.clone();
        self.local.spawn(async move {
            let result = async move {
                log::trace!("Tcp connect to {:?}", connect_string);
                let stream = async_net::TcpStream::connect(connect_string).await?;
                stream.set_nodelay(true)?;

                let (reader, writer) = stream.split();

                log::trace!("Creating capnp VatNetwork");
                let rpc_network = Box::new(twoparty::VatNetwork::new(
                    reader,
                    writer,
                    rpc_twoparty_capnp::Side::Client,
                    *ReaderOptions::new()
                        .nesting_limit(64)
                        .traversal_limit_in_words(Some(256 * 1024 * 1024)),
                ));

                let mut rpc_system = RpcSystem::new(rpc_network, None);

                let hub: asset_hub::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

                let _disconnector = rpc_system.get_disconnector();
                local.spawn(rpc_system).detach();

                log::trace!("Requesting RPC snapshot..");
                let response = hub.get_snapshot_request().send().promise.await?;

                let snapshot = response.get()?.get_snapshot()?;
                log::trace!("Received snapshot, registering listener..");
                let (snapshot_tx, snapshot_rx) = unbounded();
                let listener: asset_hub::listener::Client = capnp_rpc::new_client(ListenerImpl {
                    snapshot_channel: snapshot_tx,
                    snapshot_change: None,
                });

                let mut request = hub.register_listener_request();
                request.get().set_listener(listener);
                let rpc_conn = request.send().promise.await.map(|_| RpcConnection {
                    snapshot,
                    snapshot_rx,
                })?;
                log::trace!("Registered listener, done connecting RPC loader.");

                Ok(rpc_conn)
            }
            .await;
            let _ = conn_tx.send(result);
        }).detach();

        self.connection = InternalConnectionState::Connecting(conn_rx)
    }
}

pub struct RpcIO {
    connect_string: String,
    runtime: Mutex<RpcRuntime>,
    requests: QueuedRequests,
}

#[derive(Default)]
struct QueuedRequests {
    data_requests: Vec<DataRequest>,
    metadata_requests: Vec<MetadataRequest>,
    resolve_requests: Vec<ResolveRequest>,
}

impl Default for RpcIO {
    fn default() -> RpcIO {
        RpcIO::new("127.0.0.1:9999".to_string()).unwrap()
    }
}

impl RpcIO {
    pub fn new(connect_string: String) -> std::io::Result<RpcIO> {
        Ok(RpcIO {
            connect_string,
            runtime: Mutex::new(RpcRuntime {
                local: Rc::new(async_executor::LocalExecutor::new()),
                connection: InternalConnectionState::None,
            }),
            requests: Default::default(),
        })
    }
}

impl LoaderIO for RpcIO {
    fn get_asset_metadata_with_dependencies(&mut self, request: MetadataRequest) {
        self.requests.metadata_requests.push(request);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn get_asset_candidates(&mut self, requests: Vec<ResolveRequest>) {
        self.requests.resolve_requests.extend(requests);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn get_artifacts(&mut self, requests: Vec<DataRequest>) {
        self.requests.data_requests.extend(requests);
        let mut runtime = self.runtime.lock().unwrap();
        process_requests(&mut runtime, &mut self.requests);
    }

    fn tick(&mut self, loader: &mut LoaderState) {
        let mut runtime = self.runtime.lock().unwrap();

        match &runtime.connection {
            InternalConnectionState::Error(err) => {
                log::error!("Error connecting RpcIO: {}", err);
                runtime.connect(self.connect_string.clone());
            }
            InternalConnectionState::None => {
                runtime.connect(self.connect_string.clone());
            }
            _ => {}
        };

        process_requests(&mut runtime, &mut self.requests);

        runtime.connection =
            match std::mem::replace(&mut runtime.connection, InternalConnectionState::None) {
                // update connection state
                InternalConnectionState::Connecting(pending_connection) => {
                    match pending_connection.try_recv() {
                        Ok(connection_result) => match connection_result {
                            Ok(conn) => InternalConnectionState::Connected(conn),
                            Err(err) => InternalConnectionState::Error(err),
                        },
                        Err(async_oneshot::TryRecvError::Closed) => {
                            InternalConnectionState::Error(Box::new(
                                error::TryRecvError::Closed,
                            ))
                        }
                        Err(async_oneshot::TryRecvError::Empty(pending_connection)) => {
                            InternalConnectionState::Connecting(pending_connection)
                        }
                    }
                }
                c => c,
            };

        runtime.local.try_tick();

        runtime.check_asset_changes(loader);
    }
}

async fn do_metadata_request(
    asset: &MetadataRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<MetadataRequestResult>, capnp::Error> {
    let mut request = snapshot.get_asset_metadata_with_dependencies_request();
    let mut assets = request
        .get()
        .init_assets(asset.requested_assets().count() as u32);
    for (idx, asset) in asset.requested_assets().enumerate() {
        assets.reborrow().get(idx as u32).set_id(&asset.0);
    }
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let artifacts = reader
        .get_assets()?
        .into_iter()
        .map(|a| parse_db_metadata(&a))
        .filter(|a| a.artifact.is_some())
        .map(|a| MetadataRequestResult {
            artifact_metadata: a.artifact.clone().unwrap(),
            asset_metadata: if asset.include_asset_metadata() {
                Some(a)
            } else {
                None
            },
        })
        .collect::<Vec<_>>();
    Ok(artifacts)
}

async fn do_import_artifact_request(
    asset: &DataRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<u8>, capnp::Error> {
    let mut request = snapshot.get_import_artifacts_request();
    let mut assets = request.get().init_assets(1);
    assets.reborrow().get(0).set_id(&asset.asset_id().0);
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let artifact = reader.get_artifacts()?.get(0);
    Ok(Vec::from(artifact.get_data()?))
}

async fn do_resolve_request(
    resolve: &ResolveRequest,
    snapshot: &asset_hub::snapshot::Client,
) -> Result<Vec<(PathBuf, Vec<AssetMetadata>)>, capnp::Error> {
    let path = resolve.identifier().path();
    // get asset IDs at path
    let mut request = snapshot.get_assets_for_paths_request();
    let mut paths = request.get().init_paths(1);
    paths.reborrow().set(0, path.as_bytes());
    let response = request.send().promise.await?;
    let reader = response.get()?;
    let mut results = Vec::new();
    for reader in reader.get_assets()? {
        let path = PathBuf::from(std::str::from_utf8(reader.get_path()?)?);
        let asset_ids = reader.get_assets()?;
        // get metadata for the assetIDs
        let mut request = snapshot.get_asset_metadata_request();
        request.get().set_assets(asset_ids)?;
        let response = request.send().promise.await?;
        let reader = response.get()?;
        results.push((
            path,
            reader
                .get_assets()?
                .into_iter()
                .map(|a| parse_db_metadata(&a))
                .collect::<Vec<_>>(),
        ));
    }
    Ok(results)
}

fn process_requests(runtime: &mut RpcRuntime, requests: &mut QueuedRequests) {
    if let InternalConnectionState::Connected(connection) = &runtime.connection {
        let len = requests.data_requests.len();
        for asset in requests.data_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn(async move {
                match do_import_artifact_request(&asset, &snapshot).await {
                    Ok(data) => {
                        asset.complete(data);
                    }
                    Err(e) => {
                        asset.error(e);
                    }
                }
            }).detach();
        }

        let len = requests.metadata_requests.len();
        for m in requests.metadata_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn(async move {
                match do_metadata_request(&m, &snapshot).await {
                    Ok(data) => {
                        m.complete(data);
                    }
                    Err(e) => {
                        m.error(e);
                    }
                }
            }).detach();
        }

        let len = requests.resolve_requests.len();
        for m in requests.resolve_requests.drain(0..len) {
            let snapshot = connection.snapshot.clone();
            runtime.local.spawn(async move {
                match do_resolve_request(&m, &snapshot).await {
                    Ok(data) => {
                        m.complete(data);
                    }
                    Err(e) => {
                        m.error(e);
                    }
                }
            }).detach();
        }
    }
}

struct ListenerImpl {
    snapshot_channel: Sender<SnapshotChange>,
    snapshot_change: Option<u64>,
}
impl asset_hub::listener::Server for ListenerImpl {
    fn update(
        &mut self,
        params: asset_hub::listener::UpdateParams,
        _results: asset_hub::listener::UpdateResults,
    ) -> Promise<()> {
        let params = pry!(params.get());
        let snapshot = pry!(params.get_snapshot());
        log::trace!(
            "ListenerImpl::update self.snapshot_change: {:?}",
            self.snapshot_change
        );
        if let Some(change_num) = self.snapshot_change {
            let channel = self.snapshot_channel.clone();
            let mut request = snapshot.get_asset_changes_request();
            request.get().set_start(change_num);
            request
                .get()
                .set_count(params.get_latest_change() - change_num);
            return Promise::from_future(async move {
                let response = request.send().promise.await?;
                let response = response.get()?;

                let mut changed_assets = Vec::new();
                let mut deleted_assets = Vec::new();
                let mut changed_paths = Vec::new();
                let mut deleted_paths = Vec::new();

                for change in response.get_changes()? {
                    match change.get_event()?.which()? {
                        asset_change_event::ContentUpdateEvent(evt) => {
                            let id = utils::make_array(evt?.get_id()?.get_id()?);
                            log::trace!("ListenerImpl::update asset_change_event::ContentUpdateEvent(evt) id: {:?}", id);
                            changed_assets.push(id);
                        }
                        asset_change_event::RemoveEvent(evt) => {
                            let id = utils::make_array(evt?.get_id()?.get_id()?);
                            log::trace!(
                                "ListenerImpl::update asset_change_event::RemoveEvent(evt) id: {:?}",
                                id
                            );
                            deleted_assets.push(id);
                        }
                        asset_change_event::PathRemoveEvent(evt) => {
                            deleted_paths
                                .push(PathBuf::from(std::str::from_utf8(evt?.get_path()?)?));
                        }
                        asset_change_event::PathUpdateEvent(evt) => {
                            changed_paths
                                .push(PathBuf::from(std::str::from_utf8(evt?.get_path()?)?));
                        }
                    }
                }

                channel
                    .send(SnapshotChange {
                        snapshot,
                        changed_assets,
                        deleted_assets,
                        deleted_paths,
                        changed_paths,
                    })
                    .map_err(|_| capnp::Error::failed("Could not send SnapshotChange".into()))
            });
        } else {
            let _ = self.snapshot_channel.try_send(SnapshotChange {
                snapshot,
                changed_assets: Vec::new(),
                deleted_assets: Vec::new(),
                changed_paths: Vec::new(),
                deleted_paths: Vec::new(),
            });
        }
        self.snapshot_change = Some(params.get_latest_change());
        Promise::ok(())
    }
}
