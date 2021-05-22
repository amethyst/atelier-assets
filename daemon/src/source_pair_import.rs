use std::{
    collections::HashSet,
    fs,
    hash::{Hash, Hasher},
    io::{BufRead, Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use distill_core::{utils, ArtifactId, AssetRef, AssetTypeId, AssetUuid, CompressionType};
use distill_importer::{
    ArtifactMetadata, AssetMetadata, BoxedImporter, ExportAsset, ImportOp, ImportedAsset,
    ImporterContext, ImporterContextHandle, SerdeObj, SerializedAsset,
    SourceMetadata as ImporterSourceMetadata, SOURCEMETADATA_VERSION,
};
use distill_schema::data;
use futures::future::{BoxFuture, Future};
use log::{debug, error};
use serde::{Deserialize, Serialize};

use crate::{
    daemon::ImporterMap,
    error::{Error, Result},
    file_tracker,
    file_tracker::FileState,
    watcher::file_metadata,
};
use futures::AsyncReadExt;

pub type SourceMetadata = ImporterSourceMetadata<Box<dyn SerdeObj>, Box<dyn SerdeObj>>;

/// CachedSourceMetadata contains metadata about the inputs and results of an import operation.
#[derive(Serialize, Deserialize, Default)]
pub struct ImportResultMetadata {
    /// Hash of the source file + importer options + importer state when last importing source file.
    pub import_hash: Option<u64>,
    /// The [`crate::Importer::version`] used to import the source file.
    pub importer_version: u32,
    /// The [`TypeUuidDynamic::uuid`] used to import the source file.
    #[serde(default)]
    pub importer_type: AssetTypeId,
    /// Metadata of assets resulting from importing the source file.
    pub assets: Vec<AssetMetadata>,
}
// Only files get Some(hash)
#[derive(Clone, Debug)]
pub(crate) struct HashedSourcePair {
    pub source: Option<FileState>,
    pub source_hash: Option<u64>,
    pub meta: Option<FileState>,
    pub meta_hash: Option<u64>,
}
#[derive(Clone)]
pub(crate) struct SourcePair {
    pub source: Option<FileState>,
    pub meta: Option<FileState>,
}

pub(crate) struct PairImportResult {
    pub importer_context_set: Option<ImporterContextHandleSet>,
    pub assets: Vec<AssetImportResult>,
    pub import_op: Option<ImportOp>,
}

pub(crate) struct AssetImportResult {
    pub metadata: AssetMetadata,
    pub unresolved_load_refs: Vec<AssetRef>,
    pub unresolved_build_refs: Vec<AssetRef>,
    pub asset: Option<Box<dyn SerdeObj>>,
    pub serialized_asset: Option<SerializedAsset<Vec<u8>>>,
}

impl AssetImportResult {
    pub(crate) fn is_fully_resolved(&self) -> bool {
        self.unresolved_load_refs
            .iter()
            .find(|r| r.is_uuid())
            .is_none()
            && self
                .unresolved_build_refs
                .iter()
                .find(|r| r.is_uuid())
                .is_none()
    }
}

#[derive(Default)]
pub(crate) struct SourcePairImport<'a> {
    source: PathBuf,
    importer: Option<&'a dyn BoxedImporter>,
    importer_contexts: Option<&'a [Box<dyn ImporterContext>]>,
    source_hash: Option<u64>,
    meta_hash: Option<u64>,
    import_hash: Option<u64>,
    source_metadata: Option<SourceMetadata>,
    result_metadata: Option<ImportResultMetadata>,
}

pub(crate) trait SourceMetadataCache {
    fn restore_source_metadata(
        &self,
        path: &Path,
        importer: &dyn BoxedImporter,
        metadata: &mut SourceMetadata,
    ) -> Result<()>;
    fn get_cached_metadata(&self, path: &Path) -> Result<Option<ImportResultMetadata>>;
}

pub struct ImporterContextHandleSet(Vec<Box<dyn ImporterContextHandle>>);
impl ImporterContextHandleSet {
    pub async fn scope<F>(&mut self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        let mut out: Option<F::Output> = None;

        let mut fut: BoxFuture<'_, ()> = Box::pin(async {
            out = Some(fut.await);
        });
        for handle in &self.0 {
            fut = Box::pin(handle.scope(fut));
        }

        fut.await;
        out.expect("Future was executed in the context")
    }

    // pub fn enter(&mut self) {
    //     for handle in self.0.iter_mut() {
    //         handle.enter();
    //     }
    // }
    // pub fn exit(&mut self) {
    //     for handle in self.0.iter_mut().rev() {
    //         handle.exit();
    //     }
    // }
    pub fn resolve_ref(&mut self, asset_ref: &AssetRef, id: AssetUuid) {
        for handle in self.0.iter_mut() {
            handle.resolve_ref(asset_ref, id);
        }
    }

    pub fn begin_serialize_asset(&mut self, id: AssetUuid) {
        for handle in self.0.iter_mut() {
            handle.begin_serialize_asset(id);
        }
    }

    pub fn end_serialize_asset(&mut self, id: AssetUuid) -> HashSet<AssetRef> {
        let mut deps = HashSet::new();
        for handle in self.0.iter_mut() {
            for dep in handle.end_serialize_asset(id) {
                deps.insert(dep);
            }
        }
        deps
    }
}

impl<'a> SourcePairImport<'a> {
    pub fn new(source: PathBuf) -> SourcePairImport<'a> {
        SourcePairImport {
            source,
            ..Default::default()
        }
    }

    pub fn source_metadata(&self) -> Option<&SourceMetadata> {
        self.source_metadata.as_ref()
    }

    pub fn result_metadata(&self) -> Option<&ImportResultMetadata> {
        self.result_metadata.as_ref()
    }

    pub fn set_source_hash(&mut self, source_hash: u64) {
        self.source_hash = Some(source_hash);
    }

    pub fn set_meta_hash(&mut self, meta_hash: u64) {
        self.meta_hash = Some(meta_hash);
    }

    pub fn hash_source(&mut self) {
        let state = FileState {
            path: self.source.clone(),
            state: data::FileState::Exists,
            last_modified: 0,
            length: 0,
            ty: data::FileType::None,
        };

        hash_file(&state)
            .map(|(_, hash)| self.source_hash = hash)
            .unwrap_or_else(|err| error!("Failed to hash file: {}", err));
    }

    /// Returns true if an appropriate importer was found, otherwise false.
    pub fn set_importer_from_map(&mut self, importers: &'a ImporterMap) -> bool {
        self.importer = importers.get_by_path(&self.source);
        self.importer.is_some()
    }

    pub fn set_importer_contexts(&mut self, importer_contexts: &'a [Box<dyn ImporterContext>]) {
        self.importer_contexts = Some(importer_contexts);
    }

    pub fn needs_source_import(&mut self, scratch_buf: &mut Vec<u8>) -> Result<bool> {
        if let (Some(meta_file), Some(cached_result)) =
            (self.source_metadata.as_ref(), self.result_metadata.as_ref())
        {
            if meta_file.version != SOURCEMETADATA_VERSION {
                return Ok(true);
            }
            if cached_result.importer_version
                != self
                    .importer
                    .expect("need importer to determine if source import is required")
                    .version()
            {
                return Ok(true);
            }
            if cached_result.import_hash.is_none() {
                return Ok(true);
            }
            if self.import_hash.is_none() {
                self.import_hash = Some(self.calc_import_hash(
                    meta_file.importer_options.as_ref(),
                    meta_file.importer_state.as_ref(),
                    cached_result.importer_version,
                    cached_result.importer_type.0,
                    scratch_buf,
                )?);
            }
            Ok(self.import_hash.unwrap() != cached_result.import_hash.unwrap())
        } else {
            Ok(true)
        }
    }

    fn calc_import_hash(
        &self,
        options: &dyn SerdeObj,
        state: &dyn SerdeObj,
        importer_version: u32,
        importer_type: [u8; 16],
        scratch_buf: &mut Vec<u8>,
    ) -> Result<u64> {
        let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &options)?;
        scratch_buf.hash(&mut hasher);
        scratch_buf.clear();
        bincode::serialize_into(&mut *scratch_buf, &state)?;
        scratch_buf.hash(&mut hasher);
        self.source_hash
            .expect("cannot calculate import hash without source hash")
            .hash(&mut hasher);
        importer_version.hash(&mut hasher);
        importer_type.hash(&mut hasher);
        Ok(hasher.finish())
    }

    pub fn import_hash(&self) -> Option<u64> {
        self.import_hash
    }

    pub async fn read_metadata_from_file(&mut self, scratch_buf: &mut Vec<u8>) -> Result<()> {
        let importer = self
            .importer
            .expect("cannot read metadata without an importer");
        let meta = utils::to_meta_path(&self.source);
        let mut f = async_fs::File::open(&meta).await?;
        scratch_buf.clear();
        f.read_to_end(scratch_buf).await?;
        let mut deserializer = ron::de::Deserializer::from_bytes(&scratch_buf)?;
        let mut deserializer = <dyn erased_serde::Deserializer<'_>>::erase(&mut deserializer);
        self.source_metadata = Some(importer.deserialize_metadata(&mut deserializer)?);
        Ok(())
    }

    pub fn get_result_metadata_from_cache<C: SourceMetadataCache>(
        &mut self,
        cache: &C,
    ) -> Result<()> {
        self.result_metadata = cache.get_cached_metadata(&self.source)?;
        Ok(())
    }

    pub fn generate_source_metadata<C: SourceMetadataCache>(&mut self, metadata_cache: &C) {
        let importer = self
            .importer
            // TODO(happens): Do we need to handle this?
            .expect("cannot create metadata without an importer");

        let mut default_metadata = SourceMetadata {
            version: SOURCEMETADATA_VERSION,
            importer_options: importer.default_options(),
            importer_state: importer.default_state(),
        };

        let restored =
            metadata_cache.restore_source_metadata(&self.source, importer, &mut default_metadata);
        if restored.is_ok() {
            self.source_metadata = Some(default_metadata);
        }
    }

    fn get_importer_context_set(
        import_contexts: Option<&[Box<dyn ImporterContext>]>,
    ) -> ImporterContextHandleSet {
        let mut ctx_handles = Vec::new();
        if let Some(contexts) = import_contexts {
            for ctx in contexts.iter() {
                ctx_handles.push(ctx.handle());
            }
        }
        ImporterContextHandleSet(ctx_handles)
    }

    pub fn import_result_from_cached_data(&self) -> Result<PairImportResult> {
        let mut assets = Vec::new();
        if let Some(metadatas) = self.result_metadata.as_ref() {
            for asset in &metadatas.assets {
                use std::iter::FromIterator;
                let unresolved_load_refs: HashSet<
                    AssetRef,
                    std::collections::hash_map::RandomState,
                > = HashSet::from_iter(
                    asset
                        .artifact
                        .as_ref()
                        .map(|artifact| {
                            artifact
                                .load_deps
                                .iter()
                                .filter(|r| !r.is_uuid())
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_else(Vec::new),
                );
                let unresolved_build_refs: HashSet<
                    AssetRef,
                    std::collections::hash_map::RandomState,
                > = HashSet::from_iter(
                    asset
                        .artifact
                        .as_ref()
                        .map(|artifact| {
                            artifact
                                .build_deps
                                .iter()
                                .filter(|r| !r.is_uuid())
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_else(Vec::new),
                );
                assets.push(AssetImportResult {
                    metadata: asset.clone(),
                    unresolved_load_refs: unresolved_load_refs.into_iter().collect(),
                    unresolved_build_refs: unresolved_build_refs.into_iter().collect(),
                    asset: None,
                    serialized_asset: None,
                });
            }
        }
        Ok(PairImportResult {
            importer_context_set: None,
            assets,
            import_op: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_import_result(
        &mut self,
        op: Option<ImportOp>,
        importer: &dyn BoxedImporter,
        options: Box<dyn SerdeObj>,
        state: Box<dyn SerdeObj>,
        scratch_buf: &mut Vec<u8>,
        assets: Vec<ImportedAsset>,
        mut ctx: ImporterContextHandleSet,
    ) -> Result<PairImportResult> {
        let mut imported_assets = Vec::new();
        let import_hash = self.calc_import_hash(
            options.as_ref(),
            state.as_ref(),
            importer.version(),
            importer.uuid(),
            scratch_buf,
        )?;
        self.import_hash = Some(import_hash);
        for mut asset in assets {
            asset.search_tags.push((
                "path".to_string(),
                Some(self.source.to_string_lossy().to_string()),
            ));
            asset.search_tags.push((
                "file_name".to_string(),
                Some(
                    self.source
                        .file_name()
                        .expect("failed to get file stem")
                        .to_string_lossy()
                        .to_string(),
                ),
            ));
            ctx.begin_serialize_asset(asset.id);
            let scope_result: Result<_> = ctx
                .scope(async {
                    // We need to serialize each asset to gather references.
                    // TODO write a dummy serializer that doesn't output anything to optimize this
                    let serialized_asset = crate::serialized_asset::create(
                        0,
                        asset.id,
                        Vec::new(),
                        Vec::new(),
                        asset.asset_data.as_ref(),
                        CompressionType::None,
                        scratch_buf,
                    )?;
                    Ok((asset, serialized_asset))
                })
                .await;

            let (mut asset, serialized_asset) = scope_result?;
            let serde_refs = ctx.end_serialize_asset(asset.id);
            // TODO implement build pipeline execution
            // let build_pipeline = metadata
            //     .assets
            //     .iter()
            //     .find(|a| a.id == asset.id)
            //     .and_then(|m| m.build_pipeline);
            // Add the collected serialization dependencies to the build and load dependencies
            let mut unresolved_load_refs = Vec::new();
            let mut load_deps = HashSet::new();
            for load_dep in serde_refs.iter().chain(asset.load_deps.iter()) {
                // check insert return value to prevent duplicates in unresolved_load_refs
                if load_deps.insert(load_dep.clone()) {
                    if let AssetRef::Path(path) = load_dep {
                        unresolved_load_refs.push(AssetRef::Path(path.clone()));
                    }
                }
            }
            let mut unresolved_build_refs = Vec::new();
            let mut build_deps = HashSet::new();
            for build_dep in serde_refs
                .into_iter()
                .chain(asset.build_deps.iter().cloned())
            {
                // check insert return value to prevent duplicates in unresolved_build_refs
                if build_deps.insert(build_dep.clone()) {
                    if let AssetRef::Path(path) = build_dep {
                        unresolved_build_refs.push(AssetRef::Path(path));
                    }
                }
            }
            asset.load_deps = load_deps.into_iter().collect();
            asset.build_deps = build_deps.into_iter().collect();
            imported_assets.push(AssetImportResult {
                metadata: AssetMetadata {
                    id: asset.id,
                    search_tags: asset.search_tags,
                    artifact: Some(ArtifactMetadata {
                        asset_id: asset.id,
                        id: ArtifactId(utils::calc_import_artifact_hash(
                            &asset.id,
                            import_hash,
                            asset
                                .load_deps
                                .iter()
                                .chain(asset.build_deps.iter())
                                .filter_map(|dep| {
                                    if dep.is_uuid() {
                                        Some(dep.expect_uuid())
                                    } else {
                                        None
                                    }
                                }),
                        )),
                        load_deps: asset.load_deps.clone(),
                        build_deps: asset.build_deps.clone(),
                        compression: serialized_asset.metadata.compression,
                        compressed_size: serialized_asset.metadata.compressed_size,
                        uncompressed_size: serialized_asset.metadata.uncompressed_size,
                        type_id: AssetTypeId(asset.asset_data.uuid()),
                    }),
                    build_pipeline: asset.build_pipeline,
                },
                unresolved_load_refs,
                unresolved_build_refs,
                asset: Some(asset.asset_data),
                serialized_asset: Some(serialized_asset),
            });
        }
        self.source_metadata = Some(SourceMetadata {
            version: SOURCEMETADATA_VERSION,
            importer_options: options,
            importer_state: state,
        });
        self.result_metadata = Some(ImportResultMetadata {
            assets: imported_assets.iter().map(|a| a.metadata.clone()).collect(),
            import_hash: Some(import_hash),
            importer_version: importer.version(),
            importer_type: AssetTypeId(importer.uuid()),
        });

        Ok(PairImportResult {
            importer_context_set: Some(ctx),
            assets: imported_assets,
            import_op: op,
        })
    }

    pub async fn export_source(
        &mut self,
        scratch_buf: &mut Vec<u8>,
        assets: Vec<SerializedAsset<Vec<u8>>>,
    ) -> Result<PairImportResult> {
        let start_time = Instant::now();
        let importer = self
            .importer
            .expect("cannot export source without importer");

        let metadata = std::mem::replace(&mut self.source_metadata, None)
            .expect("cannot export source file without source_metadata");

        let mut ctx = Self::get_importer_context_set(self.importer_contexts);

        let source = &self.source;
        let exported = ctx
            .scope(async move {
                let mut f = async_fs::File::open(source).await?;
                importer
                    .export_boxed(
                        &mut f,
                        metadata.importer_options,
                        metadata.importer_state,
                        assets
                            .into_iter()
                            .map(|asset| ExportAsset { asset })
                            .collect(),
                    )
                    .await
            })
            .await?;
        self.hash_source(); // hash source to get a hash of the exported data
        let options = exported.options;
        let state = exported.state;
        let imported = exported.value;

        let result = self
            .build_import_result(
                None,
                importer,
                options,
                state,
                scratch_buf,
                imported.assets,
                ctx,
            )
            .await?;
        log::info!(
            "Exported pair in {}",
            Instant::now().duration_since(start_time).as_secs_f32()
        );
        Ok(result)
    }

    pub async fn import_source(&mut self, scratch_buf: &mut Vec<u8>) -> Result<PairImportResult> {
        log::trace!("import_source importing {:?}", &self.source);
        let start_time = Instant::now();
        let importer = self
            .importer
            .expect("cannot import source without importer");

        let metadata = std::mem::replace(&mut self.source_metadata, None)
            .expect("cannot import source file without source_metadata");

        let mut ctx = Self::get_importer_context_set(self.importer_contexts);

        let source = &self.source;

        let mut import_op = ImportOp::default();
        let import_op_ref = &mut import_op;
        let imported = ctx
            .scope(async move {
                // TODO(dvd): Can this be replaced now that tokio is gone?
                //This is broken on tokio 0.2.14 and later (concurrent file loads endlessly yield to
                // each other.
                // let mut f = File::open(source).await?;
                // let result = importer
                //     .import_boxed(&mut f, metadata.importer_options, metadata.importer_state)
                //     .await;

                // Non-async work-around
                let mut f = std::fs::File::open(source)?;
                let mut contents = vec![];
                f.read_to_end(&mut contents)?;
                let mut cursor = futures::io::Cursor::new(contents);

                importer
                    .import_boxed(
                        import_op_ref,
                        &mut cursor,
                        metadata.importer_options,
                        metadata.importer_state,
                    )
                    .await
            })
            .await?;
        log::trace!("import_source building result {:?}", self.source);
        let options = imported.options;
        let state = imported.state;
        let imported = imported.value;
        let result = self
            .build_import_result(
                Some(import_op),
                importer,
                options,
                state,
                scratch_buf,
                imported.assets,
                ctx,
            )
            .await?;
        log::info!(
            "Imported pair {:?} in {}",
            self.source,
            Instant::now().duration_since(start_time).as_secs_f32()
        );
        Ok(result)
    }

    pub fn write_metadata(&self) -> Result<()> {
        let serialized_metadata = ron::ser::to_string_pretty(
            self.source_metadata
                .as_ref()
                .expect("source_metadata missing"),
            ron::ser::PrettyConfig::default(),
        )
        .unwrap();
        let meta_path = utils::to_meta_path(&self.source);
        let mut meta_file = fs::File::create(meta_path)?;
        meta_file.write_all(serialized_metadata.as_bytes())?;
        Ok(())
    }
}

pub(crate) async fn import_pair<'a, C: SourceMetadataCache>(
    metadata_cache: &C,
    importer_map: &'a ImporterMap,
    importer_contexts: &'a [Box<dyn ImporterContext>],
    pair: &HashedSourcePair,
    scratch_buf: &mut Vec<u8>,
) -> Result<Option<(SourcePairImport<'a>, Option<PairImportResult>)>> {
    let original_pair = pair.clone();
    let mut pair = pair.clone();
    // When source or meta gets deleted, the FileState has a `state` of `Deleted`.
    // For the following pattern matching, we don't want to care about the distinction between this and absence of a file.
    if let HashedSourcePair {
        source:
            Some(FileState {
                state: data::FileState::Deleted,
                ..
            }),
        ..
    } = pair
    {
        pair.source = None;
    }
    if let HashedSourcePair {
        meta:
            Some(FileState {
                state: data::FileState::Deleted,
                ..
            }),
        ..
    } = pair
    {
        pair.meta = None;
    }

    match pair {
        // Source file has been deleted
        HashedSourcePair {
            meta: None,
            source: None,
            ..
        } => {
            if let HashedSourcePair {
                source: Some(state),
                ..
            } = original_pair
            {
                debug!("deleted pair {}", state.path.to_string_lossy());
            } else if let HashedSourcePair {
                meta: Some(state), ..
            } = original_pair
            {
                debug!("deleted pair {}", state.path.to_string_lossy());
            }
            Ok(None)
        }
        // Source file with metadata
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: Some(meta_hash),
            source: Some(source),
            source_hash: Some(source_hash),
        } => {
            debug!("full pair {}", source.path.to_string_lossy());
            let mut import = SourcePairImport::new(source.path);
            import.set_source_hash(source_hash);
            import.set_meta_hash(meta_hash);
            import.set_importer_contexts(importer_contexts);
            if !import.set_importer_from_map(&importer_map) {
                Ok(None)
            } else {
                import.read_metadata_from_file(scratch_buf).await?;
                import.get_result_metadata_from_cache(metadata_cache)?;
                if import.needs_source_import(scratch_buf)? {
                    debug!("needs source import {:?}", import.source);
                    let imported_assets = import.import_source(scratch_buf).await?;
                    import.write_metadata()?;
                    Ok(Some((import, Some(imported_assets))))
                } else {
                    debug!("does not need source import {:?}", import.source);
                    let imported_assets = import.import_result_from_cached_data()?;
                    Ok(Some((import, Some(imported_assets))))
                }
            }
        }
        // Source file with no metadata
        HashedSourcePair {
            meta: None,
            source: Some(source),
            source_hash: Some(hash),
            ..
        } => {
            debug!("file without meta {}", source.path.to_string_lossy());
            let mut import = SourcePairImport::new(source.path);
            import.set_source_hash(hash);
            import.set_importer_contexts(importer_contexts);
            if !import.set_importer_from_map(&importer_map) {
                debug!("file has no importer registered");
                Ok(Some((import, None)))
            } else {
                import.generate_source_metadata(metadata_cache);
                import.get_result_metadata_from_cache(metadata_cache)?;
                if import.needs_source_import(scratch_buf)? {
                    debug!("running importer for source file..");
                    let imported_assets = import.import_source(scratch_buf).await?;
                    import.write_metadata()?;
                    Ok(Some((import, Some(imported_assets))))
                } else {
                    debug!("using cached metadata for source file");
                    let imported_assets = import.import_result_from_cached_data()?;
                    import.write_metadata()?;
                    Ok(Some((import, Some(imported_assets))))
                }
            }
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: Some(_hash),
            source: Some(source),
            source_hash: None,
        } => {
            debug!("directory {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: None,
            source: Some(source),
            source_hash: None,
        } => {
            debug!(
                "directory with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: None,
            source: Some(source),
            source_hash: Some(_hash),
        } => {
            debug!(
                "source file with meta directory?? {}",
                source.path.to_string_lossy()
            );
            Ok(None)
        }
        HashedSourcePair {
            meta: None,
            source: Some(source),
            source_hash: None,
            ..
        } => {
            debug!("directory with no meta {}", source.path.to_string_lossy());
            Ok(None)
        }
        HashedSourcePair {
            meta: Some(meta),
            meta_hash: Some(_meta_hash),
            source: None,
            ..
        } => {
            debug!(
                "meta file without source file {}",
                meta.path.to_string_lossy()
            );
            fs::remove_file(&meta.path)?;
            Ok(None)
        }
        _ => {
            debug!("Unknown case for {:?}", pair);
            Ok(None)
        }
    }
}

fn get_path_file_state(path: PathBuf) -> Result<Option<FileState>> {
    let state = match fs::metadata(&path) {
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(Error::IO(e)),
        Ok(metadata) => Some(crate::watcher::file_metadata(&metadata)),
    };
    Ok(state.map(|metadata| FileState {
        path,
        state: data::FileState::Exists,
        last_modified: metadata.last_modified,
        length: metadata.length,
        ty: file_tracker::db_file_type(metadata.file_type),
    }))
}

pub(crate) async fn export_pair<'a, C: SourceMetadataCache>(
    assets: Vec<SerializedAsset<Vec<u8>>>,
    metadata_cache: &C,
    importer_map: &'a ImporterMap,
    importer_contexts: &'a [Box<dyn ImporterContext>],
    source_path: PathBuf,
    meta_path: PathBuf,
    scratch_buf: &mut Vec<u8>,
) -> Result<(SourcePairImport<'a>, PairImportResult)> {
    let source_state = get_path_file_state(source_path.clone())?;
    let (source_state, source_hash) = if let Some(s) = source_state {
        let (state, hash) = hash_file(&s)?;
        (Some(state), hash)
    } else {
        (None, None)
    };
    let meta_state = get_path_file_state(meta_path)?;
    let (meta_state, meta_hash) = if let Some(s) = meta_state {
        let (state, hash) = hash_file(&s)?;
        (Some(state), hash)
    } else {
        (None, None)
    };
    let pair = HashedSourcePair {
        source_hash,
        source: source_state,
        meta_hash,
        meta: meta_state,
    };
    match pair {
        // Source file path is a directory
        HashedSourcePair {
            source: Some(_),
            source_hash: None,
            ..
        } => Err(Error::Custom("Export target path is a directory".into())),
        // Meta path is a directory
        HashedSourcePair {
            meta: Some(_meta),
            meta_hash: None,
            ..
        } => Err(Error::Custom(
            "Export target .meta path is a directory".into(),
        )),
        HashedSourcePair {
            source_hash,
            meta_hash,
            ..
        } => {
            let mut op = SourcePairImport::new(source_path.clone());
            if let Some(meta_hash) = meta_hash {
                op.set_meta_hash(meta_hash);
            }
            if let Some(source_hash) = source_hash {
                op.set_source_hash(source_hash);
            }
            op.set_importer_contexts(importer_contexts);
            if !op.set_importer_from_map(&importer_map) {
                Err(Error::Custom(format!(
                    "no importer registered for extension {:?}",
                    source_path.extension()
                )))
            } else {
                if op.needs_source_import(scratch_buf)? {
                    // if we can't use cached metadata, read from file
                    op.read_metadata_from_file(scratch_buf).await?;
                } else {
                    // read metadata from db cache
                    op.generate_source_metadata(metadata_cache);
                }
                let exported_assets = op.export_source(scratch_buf, assets).await?;
                op.write_metadata()?;
                Ok((op, exported_assets))
            }
        }
    }
}

pub(crate) fn hash_file(state: &FileState) -> Result<(FileState, Option<u64>)> {
    let metadata = match fs::metadata(&state.path) {
        Err(e) => return Err(Error::IO(e)),
        Ok(m) => {
            if !m.is_file() {
                return Ok((state.clone(), None));
            }
            file_metadata(&m)
        }
    };
    fs::OpenOptions::new()
        .read(true)
        .open(&state.path)
        .and_then(|f| {
            let mut hasher = ::std::collections::hash_map::DefaultHasher::new();
            let mut reader = ::std::io::BufReader::with_capacity(64000, f);
            loop {
                let length = {
                    let buffer = reader.fill_buf()?;
                    hasher.write(buffer);
                    buffer.len()
                };
                if length == 0 {
                    break;
                }
                reader.consume(length);
            }
            Ok((
                FileState {
                    path: state.path.clone(),
                    state: data::FileState::Exists,
                    last_modified: metadata.last_modified,
                    length: metadata.length,
                    ty: file_tracker::db_file_type(metadata.file_type),
                },
                Some(hasher.finish()),
            ))
        })
        .map_err(Error::IO)
}
