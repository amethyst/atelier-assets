use std::{cell::RefCell, path::PathBuf, rc::Rc, time::Instant};

use async_trait::async_trait;
use capnp::message::ReaderOptions;
use capnp_rpc::{pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use distill_schema::{
    data, pack,
    service::asset_hub::{self, snapshot::Client as Snapshot},
};

pub mod shell;
use shell::Autocomplete;
pub use shell::Command;
use futures::AsyncReadExt;

type Promise<T> = capnp::capability::Promise<T, capnp::Error>;
pub type DynResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct ListenerImpl {
    snapshot: Rc<RefCell<Snapshot>>,
}
impl asset_hub::listener::Server for ListenerImpl {
    fn update(
        &mut self,
        params: asset_hub::listener::UpdateParams,
        _results: asset_hub::listener::UpdateResults,
    ) -> Promise<()> {
        let snapshot = pry!(pry!(params.get()).get_snapshot());
        self.snapshot.replace(snapshot);
        Promise::ok(())
    }
}

pub fn make_array<A, T>(slice: &[T]) -> A
where
    A: Sized + Default + AsMut<[T]>,
    T: Copy,
{
    let mut a = Default::default();
    <A as AsMut<[T]>>::as_mut(&mut a).copy_from_slice(slice);
    a
}

#[allow(dead_code)]
fn endpoint() -> String {
    if cfg!(windows) {
        r"\\.\pipe\distill".to_string()
    } else {
        r"/tmp/distill".to_string()
    }
}
pub struct Context {
    snapshot: Rc<RefCell<Snapshot>>,
}

pub async fn create_context(local: &async_executor::LocalExecutor<'_>) -> Result<Context, Box<dyn std::error::Error>> {
    use std::net::ToSocketAddrs;
    let addr = "127.0.0.1:9999".to_socket_addrs()?.next().unwrap();
    let stream = async_net::TcpStream::connect(&addr).await?;
    stream.set_nodelay(true).unwrap();

    let (reader, writer) = stream.split();
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

    let snapshot = Rc::new(RefCell::new({
        let request = hub.get_snapshot_request();
        request.send().promise.await?.get()?.get_snapshot()?
    }));

    let listener: asset_hub::listener::Client = capnp_rpc::new_client(ListenerImpl {
        snapshot: snapshot.clone(),
    });

    let mut request = hub.register_listener_request();
    request.get().set_listener(listener);

    request.send().promise.await?;
    Ok(Context { snapshot })
}

pub struct CmdPack;
#[async_trait(?Send)]
impl Command<Context> for CmdPack {
    fn desc(&self) -> &str {
        "<path> - Pack artifacts into a file"
    }

    fn nargs(&self) -> usize {
        1
    }

    async fn run(&self, ctx: &Context, args: Vec<&str>) -> DynResult {
        let mut out_file = std::fs::File::create(PathBuf::from(
            args.get(0).expect("Expected file output path"),
        ))?;
        let start = Instant::now();
        let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
        let response = request.send().promise.await?;
        let response = response.get()?;
        let assets = response.get_assets()?;
        let mut message = capnp::message::Builder::new_default();
        let packfile_builder = message.init_root::<pack::pack_file::Builder>();
        let mut num_valid_entries = 0;
        let mut num_bytes = 0;
        for asset in assets {
            if matches!(
                asset.get_latest_artifact().which()?,
                data::asset_metadata::latest_artifact::Artifact(Ok(_))
            ) {
                num_valid_entries += 1;
            }
        }
        let mut packfile_entries = packfile_builder.init_entries(num_valid_entries);
        let mut i = 0;
        for asset in assets {
            if let data::asset_metadata::latest_artifact::Artifact(Ok(_)) =
                asset.get_latest_artifact().which()?
            {
                let path_response = {
                    let mut path_request = ctx.snapshot.borrow().get_path_for_assets_request();
                    let req_list = path_request.get().init_assets(1);
                    req_list.set_with_caveats(0, asset.get_id()?)?;
                    path_request.send().promise.await?
                };
                let path = path_response.get()?.get_paths()?.get(0);
                let artifact_response = {
                    let mut artifact_request = ctx.snapshot.borrow().get_import_artifacts_request();
                    let req_list = artifact_request.get().init_assets(1);
                    req_list.set_with_caveats(0, asset.get_id()?)?;
                    artifact_request.send().promise.await?
                };
                let artifact = artifact_response.get()?.get_artifacts()?.get(0);
                let mut packfile_entry = packfile_entries.reborrow().get(i);
                packfile_entry.set_asset_metadata(asset)?;
                packfile_entry.set_artifact(artifact)?;
                packfile_entry.set_path(path.get_path()?);
                num_bytes += artifact.get_data()?.len();
                // println!("path {:?}", std::str::from_utf8(path.get_path()?).unwrap());
                i += 1;
            }
        }
        capnp::serialize::write_message(&mut out_file, &message)?;
        out_file.sync_all().unwrap();
        let total_time = Instant::now().duration_since(start);
        println!(
            "packed {} assets and {} MB in {}\r",
            num_valid_entries,
            num_bytes / 1_000_000,
            total_time.as_secs_f32(),
        );
        Ok(())
    }
}

pub struct CmdShowAll;
#[async_trait(?Send)]
impl Command<Context> for CmdShowAll {
    fn desc(&self) -> &str {
        "- Get all asset metadata"
    }

    async fn run(&self, ctx: &Context, _args: Vec<&str>) -> DynResult {
        let start = Instant::now();
        let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
        let response = request.send().promise.await?;
        let response = response.get()?;
        let total_time = Instant::now().duration_since(start);
        let assets = response.get_assets()?;
        for asset in assets {
            let id = asset.get_id().unwrap().get_id().unwrap();
            println!("{:?}\r", uuid::Uuid::from_bytes(make_array(id)));
        }
        println!(
            "got {} assets in {}\r",
            assets.len(),
            total_time.as_secs_f32(),
        );
        Ok(())
    }
}

pub struct CmdGet;
#[async_trait(?Send)]
impl Command<Context> for CmdGet {
    fn desc(&self) -> &str {
        "<uuid> - Get asset metadata from uuid"
    }

    fn nargs(&self) -> usize {
        1
    }

    async fn run(&self, ctx: &Context, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_asset_metadata_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let assets = response.get_assets()?;
        for asset in assets {
            print_asset_metadata(&asset).await?;
        }
        println!(
            "got {} assets in {}\r",
            assets.len(),
            total_time.as_secs_f32(),
        );
        Ok(())
    }

    async fn autocomplete(
        &self,
        ctx: &Context,
        args: Vec<&str>,
        whitespaces_last: usize,
    ) -> DynResult<Autocomplete> {
        if args.len() > 1 {
            return Ok(Autocomplete::empty());
        }
        autocomplete_asset_uuids(ctx, args.last().copied(), whitespaces_last).await
    }
}

async fn print_asset_metadata(asset: &data::asset_metadata::Reader<'_>) -> DynResult {
    print!(
        "{{ id: {:?}",
        uuid::Uuid::from_bytes(make_array(asset.get_id().unwrap().get_id().unwrap())),
    );

    if let Ok(tags) = asset.get_search_tags() {
        let tags: Vec<String> = tags
            .iter()
            .map(|t| {
                format!(
                    "\"{}\": \"{}\"",
                    std::str::from_utf8(t.get_key().unwrap_or(b"")).unwrap(),
                    std::str::from_utf8(t.get_value().unwrap_or(b"")).unwrap()
                )
            })
            .collect();
        if !tags.is_empty() {
            print!(", search_tags: [ {} ]", tags.join(", "));
        }
    }
    println!(" }}\r");
    Ok(())
}

pub struct CmdBuild;
#[async_trait(?Send)]
impl Command<Context> for CmdBuild {
    fn desc(&self) -> &str {
        "<uuid> - Get build artifact from uuid"
    }

    fn nargs(&self) -> usize {
        1
    }

    async fn run(&self, ctx: &Context, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_import_artifacts_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let artifacts = response.get_artifacts()?;
        for artifact in artifacts {
            let asset_uuid =
                uuid::Uuid::from_slice(artifact.get_metadata()?.get_asset_id()?.get_id()?)?;
            println!(
                "{{ id: {}, hash: {:?}, length: {} }}\r",
                asset_uuid,
                artifact.get_metadata()?.get_hash()?,
                artifact.get_data()?.len()
            );
        }
        println!(
            "got {} artifacts in {}\r",
            artifacts.len(),
            total_time.as_secs_f32(),
        );
        Ok(())
    }

    async fn autocomplete(
        &self,
        ctx: &Context,
        args: Vec<&str>,
        whitespaces_last: usize,
    ) -> DynResult<Autocomplete> {
        if args.len() > 1 {
            return Ok(Autocomplete::empty());
        }
        autocomplete_asset_uuids(ctx, args.last().copied(), whitespaces_last).await
    }
}

pub struct CmdPathForAsset;
#[async_trait(?Send)]
impl Command<Context> for CmdPathForAsset {
    fn desc(&self) -> &str {
        "<uuid> - Get path from asset uuid"
    }

    fn nargs(&self) -> usize {
        1
    }

    async fn run(&self, ctx: &Context, args: Vec<&str>) -> DynResult {
        let id = uuid::Uuid::parse_str(args[0])?;
        let mut request = ctx.snapshot.borrow().get_path_for_assets_request();
        request.get().init_assets(1).get(0).set_id(id.as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let total_time = Instant::now().duration_since(start);
        let response = response.get()?;
        let asset_paths = response.get_paths()?;
        for asset_path in asset_paths {
            let asset_uuid = uuid::Uuid::from_slice(asset_path.get_id()?.get_id()?)?;
            println!(
                "{{ asset: {}, path: {} }}\r",
                asset_uuid,
                std::str::from_utf8(asset_path.get_path()?)?
            );
        }
        println!(
            "got {} asset paths in {}\r",
            asset_paths.len(),
            total_time.as_secs_f32(),
        );
        Ok(())
    }

    async fn autocomplete(
        &self,
        ctx: &Context,
        args: Vec<&str>,
        whitespaces_last: usize,
    ) -> DynResult<Autocomplete> {
        if args.len() > 1 {
            return Ok(Autocomplete::empty());
        }
        autocomplete_asset_uuids(ctx, args.last().copied(), whitespaces_last).await
    }
}

pub struct CmdAssetsForPath;
#[async_trait(?Send)]
impl Command<Context> for CmdAssetsForPath {
    fn desc(&self) -> &str {
        "<path> - Get asset metadata from path"
    }

    fn nargs(&self) -> usize {
        1
    }

    async fn run(&self, ctx: &Context, args: Vec<&str>) -> DynResult {
        let mut request = ctx.snapshot.borrow().get_assets_for_paths_request();
        request.get().init_paths(1).set(0, args[0].as_bytes());
        let start = Instant::now();
        let response = request.send().promise.await?;
        let response = response.get()?;
        let asset_uuids_to_get: Vec<_> = response
            .get_assets()?
            .iter()
            .flat_map(|a| a.get_assets().unwrap())
            .collect();

        let mut request = ctx.snapshot.borrow().get_asset_metadata_request();
        let mut assets = request.get().init_assets(asset_uuids_to_get.len() as u32);
        for (idx, asset) in asset_uuids_to_get.iter().enumerate() {
            assets
                .reborrow()
                .get(idx as u32)
                .set_id(asset.get_id().unwrap());
        }
        let response = request.send().promise.await?;
        let response = response.get()?;
        let total_time = Instant::now().duration_since(start);

        let assets = response.get_assets()?;
        for asset in assets {
            print_asset_metadata(&asset).await?;
        }
        println!(
            "got {} assets in {}\r",
            assets.len(),
            total_time.as_secs_f32(),
        );
        Ok(())
    }

    async fn autocomplete(
        &self,
        ctx: &Context,
        args: Vec<&str>,
        whitespaces_last: usize,
    ) -> DynResult<Autocomplete> {
        if args.len() > 1 {
            return Ok(Autocomplete::empty());
        }
        autocomplete_asset_paths(ctx, args.last().copied(), whitespaces_last).await
    }
}

async fn autocomplete_asset_paths(
    ctx: &Context,
    starting_str: Option<&str>,
    whitespaces_last: usize,
) -> DynResult<Autocomplete> {
    let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
    let response = request.send().promise.await?;
    let response = response.get()?;
    let assets = response.get_assets()?;

    let mut request = ctx.snapshot.borrow().get_path_for_assets_request();
    let mut req_assets = request.get().init_assets(assets.len());
    for (i, asset) in assets.iter().enumerate() {
        let id = asset.get_id()?.get_id()?;
        req_assets.reborrow().get(i as u32).set_id(id);
    }
    let response = request.send().promise.await?;
    let response = response.get()?;
    let asset_paths = response.get_paths()?;

    let mut items = Vec::new();
    if let Some(starting_str) = starting_str {
        for asset_path in asset_paths {
            let path = std::str::from_utf8(asset_path.get_path()?)?;
            if path.starts_with(starting_str) {
                items.push(path.to_owned());
            }
        }
    } else {
        for asset_path in asset_paths {
            let path = std::str::from_utf8(asset_path.get_path()?)?;
            items.push(path.to_owned());
        }
    }

    Ok(Autocomplete {
        overlap: starting_str
            .map(|s| s.len() + whitespaces_last)
            .unwrap_or(0),
        items,
    })
}

async fn autocomplete_asset_uuids(
    ctx: &Context,
    starting_str: Option<&str>,
    whitespaces_last: usize,
) -> DynResult<Autocomplete> {
    let request = ctx.snapshot.borrow().get_all_asset_metadata_request();
    let response = request.send().promise.await?;
    let response = response.get()?;
    let assets = response.get_assets()?;

    let mut items = Vec::new();
    if let Some(starting_str) = starting_str {
        for asset in assets.iter() {
            let asset_uuid = uuid::Uuid::from_slice(asset.get_id()?.get_id()?)?;
            let formatted = format!("{}", asset_uuid);
            if formatted.starts_with(starting_str) {
                items.push(formatted);
            }
        }
    } else {
        for asset in assets.iter() {
            let asset_uuid = uuid::Uuid::from_slice(asset.get_id()?.get_id()?)?;
            items.push(format!("{}", asset_uuid));
        }
    }

    Ok(Autocomplete {
        overlap: starting_str
            .map(|s| s.len() + whitespaces_last)
            .unwrap_or(0),
        items,
    })
}
