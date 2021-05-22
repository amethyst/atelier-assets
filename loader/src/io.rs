use std::{collections::HashMap, path::PathBuf};

use crossbeam_channel::Sender;
use distill_core::{ArtifactId, ArtifactMetadata, AssetMetadata, AssetUuid};

use crate::{loader::LoaderState, storage::IndirectIdentifier, LoadHandle, Result};

/// Provides [`Loader`](crate::loader::Loader) with data.
pub trait LoaderIO: Send + Sync {
    fn get_asset_metadata_with_dependencies(&mut self, request: MetadataRequest);
    fn get_asset_candidates(&mut self, requests: Vec<ResolveRequest>);
    fn get_artifacts(&mut self, requests: Vec<DataRequest>);
    fn tick(&mut self, loader: &mut LoaderState);
}

/// A request for an asset artifact's data.
pub struct DataRequest {
    pub(crate) tx: Sender<(Result<Vec<u8>>, LoadHandle, u32)>,
    pub(crate) asset_id: AssetUuid,
    pub(crate) artifact_id: ArtifactId,
    pub(crate) request_data: Option<(LoadHandle, u32)>,
}
impl DataRequest {
    pub fn asset_id(&self) -> AssetUuid {
        self.asset_id
    }

    pub fn artifact_id(&self) -> ArtifactId {
        self.artifact_id
    }

    pub fn error<T: std::error::Error + Send + 'static>(mut self, err: T) {
        if let Some(request_data) = self.request_data.take() {
            let _ = self
                .tx
                .send((Err(Box::new(err)), request_data.0, request_data.1));
        }
    }

    pub fn complete(mut self, data: Vec<u8>) {
        if let Some(request_data) = self.request_data.take() {
            let _ = self.tx.send((Ok(data), request_data.0, request_data.1));
        }
    }
}
impl Drop for DataRequest {
    fn drop(&mut self) {
        if let Some(request_data) = self.request_data.take() {
            let _ = self.tx.send((
                Err(Box::new(RequestDropError)),
                request_data.0,
                request_data.1,
            ));
        }
    }
}

/// A request for possible candidates for an [`IndirectIdentifier`].
#[allow(clippy::type_complexity)]
pub struct ResolveRequest {
    pub(crate) tx: Sender<(
        Result<Vec<(PathBuf, Vec<AssetMetadata>)>>,
        IndirectIdentifier,
        LoadHandle,
    )>,
    pub(crate) id: Option<(IndirectIdentifier, LoadHandle)>,
}
impl ResolveRequest {
    pub fn identifier(&self) -> &IndirectIdentifier {
        self.id.as_ref().map(|v| &v.0).unwrap()
    }

    pub fn error<T: std::error::Error + Send + 'static>(mut self, err: T) {
        if let Some(id) = self.id.take() {
            let _ = self.tx.send((Err(Box::new(err)), id.0, id.1));
        }
    }

    pub fn complete(mut self, data: Vec<(PathBuf, Vec<AssetMetadata>)>) {
        if let Some(id) = self.id.take() {
            let _ = self.tx.send((Ok(data), id.0, id.1));
        }
    }
}
impl Drop for ResolveRequest {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let _ = self.tx.send((Err(Box::new(RequestDropError)), id.0, id.1));
        }
    }
}
#[derive(Debug)]
struct RequestDropError;
impl std::fmt::Display for RequestDropError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("request dropped")
    }
}
impl std::error::Error for RequestDropError {}

pub struct MetadataRequestResult {
    pub artifact_metadata: ArtifactMetadata,
    pub asset_metadata: Option<AssetMetadata>,
}
/// A request for artifact metadata covering the dependency graphs of the requested asset IDs.
#[allow(clippy::type_complexity)]
pub struct MetadataRequest {
    pub(crate) tx: Sender<(
        Result<Vec<MetadataRequestResult>>,
        HashMap<AssetUuid, (LoadHandle, u32)>,
    )>,
    pub(crate) requests: Option<HashMap<AssetUuid, (LoadHandle, u32)>>,
    pub(crate) include_asset_metadata: bool,
}
impl MetadataRequest {
    pub fn requested_assets(&self) -> impl Iterator<Item = &AssetUuid> {
        self.requests.as_ref().unwrap().keys()
    }

    /// Whether the response should include asset metadata or not, for debugging purposes.
    pub fn include_asset_metadata(&self) -> bool {
        self.include_asset_metadata
    }

    pub fn error<T: std::error::Error + Send + 'static>(mut self, err: T) {
        if let Some(requests) = self.requests.take() {
            let _ = self.tx.send((Err(Box::new(err)), requests));
        }
    }

    pub fn complete(mut self, metadata: Vec<MetadataRequestResult>) {
        if let Some(requests) = self.requests.take() {
            let _ = self.tx.send((Ok(metadata), requests));
        }
    }
}

impl Drop for MetadataRequest {
    fn drop(&mut self) {
        if let Some(requests) = self.requests.take() {
            let _ = self.tx.send((Err(Box::new(RequestDropError)), requests));
        }
    }
}
