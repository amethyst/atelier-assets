use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use futures_core::future::{BoxFuture, Future};
use serde::{
    de::{self, Deserialize, Visitor},
    ser::{self, Serialize, Serializer},
};

use crate::{
    storage::{LoadStatus, LoaderInfoProvider},
    AssetRef, AssetUuid, LoadHandle, Loader,
};

/// Operations on an asset reference.
#[derive(Debug)]
pub enum RefOp {
    Decrease(LoadHandle),
    Increase(LoadHandle),
    IncreaseUuid(AssetUuid),
}

pub fn process_ref_ops(loader: &Loader, rx: &Receiver<RefOp>) {
    loop {
        match rx.try_recv() {
            Err(_) => break,
            Ok(RefOp::Decrease(handle)) => loader.remove_ref(handle),
            Ok(RefOp::Increase(handle)) => {
                loader.add_ref_handle(handle);
            }
            Ok(RefOp::IncreaseUuid(uuid)) => {
                loader.add_ref(uuid);
            }
        }
    }
}

/// Keeps track of whether a handle ref is a strong, weak or "internal" ref
#[derive(Debug)]
pub enum HandleRefType {
    /// Strong references decrement the count on drop
    Strong(Sender<RefOp>),
    /// Weak references do nothing on drop.
    Weak(Sender<RefOp>),
    /// Internal references do nothing on drop, but turn into Strong references on clone.
    /// Should only be used for references stored in loaded assets to avoid self-referencing
    Internal(Sender<RefOp>),
    /// Implementation detail, used when changing state in this enum
    None,
}

struct HandleRef {
    id: LoadHandle,
    ref_type: HandleRefType,
}
impl PartialEq for HandleRef {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}
impl Hash for HandleRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl Eq for HandleRef {}
impl Debug for HandleRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)
    }
}

impl Drop for HandleRef {
    fn drop(&mut self) {
        use HandleRefType::*;
        self.ref_type = match std::mem::replace(&mut self.ref_type, None) {
            Strong(sender) => {
                let _ = sender.send(RefOp::Decrease(self.id));
                Weak(sender)
            }
            r => r,
        };
    }
}

impl Clone for HandleRef {
    fn clone(&self) -> Self {
        use HandleRefType::*;
        Self {
            id: self.id,
            ref_type: match &self.ref_type {
                Internal(sender) | Strong(sender) => {
                    let _ = sender.send(RefOp::Increase(self.id));
                    Strong(sender.clone())
                }
                Weak(sender) => Weak(sender.clone()),
                None => panic!("unexpected ref type in clone()"),
            },
        }
    }
}

impl AssetHandle for HandleRef {
    fn load_handle(&self) -> LoadHandle {
        self.id
    }
}

/// Handle to an asset.
#[derive(Eq)]
pub struct Handle<T: ?Sized> {
    handle_ref: HandleRef,
    marker: PhantomData<T>,
}

impl<T: ?Sized> PartialEq for Handle<T> {
    fn eq(&self, other: &Self) -> bool {
        self.handle_ref == other.handle_ref
    }
}

impl<T: ?Sized> Clone for Handle<T> {
    fn clone(&self) -> Self {
        Self {
            handle_ref: self.handle_ref.clone(),
            marker: PhantomData,
        }
    }
}

impl<T: ?Sized> Hash for Handle<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.handle_ref.hash(state);
    }
}

impl<T: ?Sized> Debug for Handle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("handle_ref", &self.handle_ref)
            .finish()
    }
}

impl<T: ?Sized> From<GenericHandle> for Handle<T> {
    fn from(handle: GenericHandle) -> Self {
        Self {
            handle_ref: handle.handle_ref,
            marker: PhantomData,
        }
    }
}

impl<T> Handle<T> {
    /// Creates a new handle with `HandleRefType::Strong`
    pub fn new(chan: Sender<RefOp>, handle: LoadHandle) -> Self {
        Self {
            handle_ref: HandleRef {
                id: handle,
                ref_type: HandleRefType::Strong(chan),
            },
            marker: PhantomData,
        }
    }

    /// Creates a new handle with `HandleRefType::Internal`
    pub(crate) fn new_internal(chan: Sender<RefOp>, handle: LoadHandle) -> Self {
        Self {
            handle_ref: HandleRef {
                id: handle,
                ref_type: HandleRefType::Internal(chan),
            },
            marker: PhantomData,
        }
    }

    pub fn asset<'a>(&self, storage: &'a impl TypedAssetStorage<T>) -> Option<&'a T> {
        AssetHandle::asset(self, storage)
    }
}

impl<T> AssetHandle for Handle<T> {
    fn load_handle(&self) -> LoadHandle {
        self.handle_ref.load_handle()
    }
}

/// Handle to an asset whose type is unknown during loading.
///
/// This is returned by `Loader::load_asset_generic` for assets loaded by UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GenericHandle {
    handle_ref: HandleRef,
}

impl GenericHandle {
    /// Creates a new handle with `HandleRefType::Strong`
    pub fn new(chan: Sender<RefOp>, handle: LoadHandle) -> Self {
        Self {
            handle_ref: HandleRef {
                id: handle,
                ref_type: HandleRefType::Strong(chan),
            },
        }
    }

    /// Creates a new handle with `HandleRefType::Internal`
    pub(crate) fn new_internal(chan: Sender<RefOp>, handle: LoadHandle) -> Self {
        Self {
            handle_ref: HandleRef {
                id: handle,
                ref_type: HandleRefType::Internal(chan),
            },
        }
    }
}

impl AssetHandle for GenericHandle {
    fn load_handle(&self) -> LoadHandle {
        self.handle_ref.load_handle()
    }
}

impl<T: ?Sized> From<Handle<T>> for GenericHandle {
    fn from(handle: Handle<T>) -> Self {
        Self {
            handle_ref: handle.handle_ref,
        }
    }
}

/// Handle to an asset that does not prevent the asset from being unloaded.
///
/// Weak handles are primarily used when you want to use something that is already loaded.
///
/// For example, a strong handle to an asset may be guaranteed to exist elsewhere in the program,
/// and so you can simply get and use a weak handle to that asset in other parts of your code. This
/// removes reference counting overhead, but also ensures that the system which uses the weak handle
/// is not in control of when to unload the asset.
#[derive(Clone, Eq, Hash, PartialEq, Debug)]
pub struct WeakHandle {
    id: LoadHandle,
}

impl WeakHandle {
    pub fn new(handle: LoadHandle) -> Self {
        WeakHandle { id: handle }
    }
}

impl AssetHandle for WeakHandle {
    fn load_handle(&self) -> LoadHandle {
        self.id
    }
}

crate::task_local! {
    static LOADER: &'static dyn LoaderInfoProvider;
    static REFOP_SENDER: Sender<RefOp>;
}

/// Used to make some limited Loader interactions available to `serde` Serialize/Deserialize
/// implementations by using thread-local storage. Required to support Serialize/Deserialize of Handle.
pub struct SerdeContext;
impl SerdeContext {
    pub fn with_active<R>(f: impl FnOnce(&dyn LoaderInfoProvider, &Sender<RefOp>) -> R) -> R {
        LOADER.with(|l| REFOP_SENDER.with(|r| f(*l, &r)))
    }

    pub async fn with<F>(loader: &dyn LoaderInfoProvider, sender: Sender<RefOp>, f: F) -> F::Output
    where
        F: Future,
    {
        // The loader lifetime needs to be transmuted to 'static to be able to be stored in task_local.
        // This is safe since SerdeContext's lifetime cannot be shorter than the opened scope, and the loader
        // must live at least as long.
        let loader = unsafe {
            std::mem::transmute::<&dyn LoaderInfoProvider, &'static dyn LoaderInfoProvider>(loader)
        };

        LOADER.scope(loader, REFOP_SENDER.scope(sender, f)).await
    }
}

/// This context can be used to maintain AssetUuid references through a serialize/deserialize cycle
/// even if the LoadHandles produced are invalid. This is useful when a loader is not
/// present, such as when processing in the Distill Daemon.
struct DummySerdeContext {
    maps: RwLock<DummySerdeContextMaps>,
    current: Mutex<DummySerdeContextCurrent>,
    ref_sender: Sender<RefOp>,
    handle_gen: AtomicU64,
}

struct DummySerdeContextMaps {
    uuid_to_load: HashMap<AssetRef, LoadHandle>,
    load_to_uuid: HashMap<LoadHandle, AssetRef>,
}

struct DummySerdeContextCurrent {
    current_serde_dependencies: HashSet<AssetRef>,
    current_serde_asset: Option<AssetUuid>,
}

impl DummySerdeContext {
    pub fn new() -> Self {
        let (tx, _) = unbounded();
        Self {
            maps: RwLock::new(DummySerdeContextMaps {
                uuid_to_load: HashMap::default(),
                load_to_uuid: HashMap::default(),
            }),
            current: Mutex::new(DummySerdeContextCurrent {
                current_serde_dependencies: HashSet::new(),
                current_serde_asset: None,
            }),
            ref_sender: tx,
            handle_gen: AtomicU64::new(1),
        }
    }
}

impl LoaderInfoProvider for DummySerdeContext {
    fn get_load_handle(&self, asset_ref: &AssetRef) -> Option<LoadHandle> {
        let mut maps = self.maps.write().unwrap();
        let maps = &mut *maps;
        let uuid_to_load = &mut maps.uuid_to_load;
        let load_to_uuid = &mut maps.load_to_uuid;

        let entry = uuid_to_load.entry(asset_ref.clone());
        let handle = entry.or_insert_with(|| {
            let new_id = self.handle_gen.fetch_add(1, Ordering::Relaxed);
            let handle = LoadHandle(new_id);
            load_to_uuid.insert(handle, asset_ref.clone());
            handle
        });

        Some(*handle)
    }

    fn get_asset_id(&self, load: LoadHandle) -> Option<AssetUuid> {
        let maps = self.maps.read().unwrap();
        let maybe_asset = maps.load_to_uuid.get(&load).cloned();
        if let Some(asset_ref) = maybe_asset.as_ref() {
            let mut current = self.current.lock().unwrap();
            if let Some(ref current_serde_id) = current.current_serde_asset {
                if AssetRef::Uuid(*current_serde_id) != *asset_ref
                    && *asset_ref != AssetRef::Uuid(AssetUuid::default())
                {
                    current.current_serde_dependencies.insert(asset_ref.clone());
                }
            }
        }
        if let Some(AssetRef::Uuid(uuid)) = maybe_asset {
            Some(uuid)
        } else {
            None
        }
    }
}
struct DummySerdeContextHandle {
    dummy: Arc<DummySerdeContext>,
}
impl<'a> distill_core::importer_context::ImporterContextHandle for DummySerdeContextHandle {
    fn scope<'s>(&'s self, fut: BoxFuture<'s, ()>) -> BoxFuture<'s, ()> {
        let sender = self.dummy.ref_sender.clone();
        let loader = &*self.dummy;
        Box::pin(SerdeContext::with(loader, sender, fut))
    }

    fn resolve_ref(&mut self, asset_ref: &AssetRef, asset: AssetUuid) {
        let new_ref = AssetRef::Uuid(asset);
        let mut maps = self.dummy.maps.write().unwrap();
        if let Some(handle) = maps.uuid_to_load.get(asset_ref) {
            let handle = *handle;
            maps.load_to_uuid.insert(handle, new_ref.clone());
            maps.uuid_to_load.insert(new_ref, handle);
        }
    }

    /// Begin gathering dependencies for an asset
    fn begin_serialize_asset(&mut self, asset: AssetUuid) {
        let mut current = self.dummy.current.lock().unwrap();
        if current.current_serde_asset.is_some() {
            panic!("begin_serialize_asset when current_serde_asset is already set");
        }
        current.current_serde_asset = Some(asset);
    }

    /// Finish gathering dependencies for an asset
    fn end_serialize_asset(&mut self, _asset: AssetUuid) -> HashSet<AssetRef> {
        let mut current = self.dummy.current.lock().unwrap();
        if current.current_serde_asset.is_none() {
            panic!("end_serialize_asset when current_serde_asset is not set");
        }
        current.current_serde_asset = None;
        std::mem::replace(&mut current.current_serde_dependencies, HashSet::new())
    }
}

/// Register this context with AssetDaemon to add serde support for Handle.
pub struct HandleSerdeContextProvider;
impl distill_core::importer_context::ImporterContext for HandleSerdeContextProvider {
    fn handle(&self) -> Box<dyn distill_core::importer_context::ImporterContextHandle> {
        let dummy = Arc::new(DummySerdeContext::new());
        Box::new(DummySerdeContextHandle { dummy })
    }
}

fn serialize_handle<S>(load: LoadHandle, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SerdeContext::with_active(|loader, _| {
        use ser::SerializeSeq;
        let uuid: AssetUuid = loader.get_asset_id(load).unwrap_or_default();
        let mut seq = serializer.serialize_seq(Some(uuid.0.len()))?;
        for element in &uuid.0 {
            seq.serialize_element(element)?;
        }
        seq.end()
    })
}
impl<T> Serialize for Handle<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_handle(self.handle_ref.id, serializer)
    }
}
impl Serialize for GenericHandle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_handle(self.handle_ref.id, serializer)
    }
}

fn get_handle_ref(asset_ref: AssetRef) -> (LoadHandle, Sender<RefOp>) {
    SerdeContext::with_active(|loader, sender| {
        let handle = if asset_ref == AssetRef::Uuid(AssetUuid::default()) {
            LoadHandle(0)
        } else {
            loader
                .get_load_handle(&asset_ref)
                .unwrap_or_else(|| panic!("Handle for AssetUuid {:?} was not present when deserializing a Handle. This indicates missing dependency metadata, and can be caused by dependency cycles.", asset_ref))
        };
        (handle, sender.clone())
    })
}

impl<'de, T> Deserialize<'de> for Handle<T> {
    fn deserialize<D>(deserializer: D) -> Result<Handle<T>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let asset_ref = if deserializer.is_human_readable() {
            deserializer.deserialize_any(AssetRefVisitor)?
        } else {
            deserializer.deserialize_seq(AssetRefVisitor)?
        };
        let (handle, sender) = get_handle_ref(asset_ref);
        Ok(Handle::new_internal(sender, handle))
    }
}

impl<'de> Deserialize<'de> for GenericHandle {
    fn deserialize<D>(deserializer: D) -> Result<GenericHandle, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let asset_ref = if deserializer.is_human_readable() {
            deserializer.deserialize_any(AssetRefVisitor)?
        } else {
            deserializer.deserialize_seq(AssetRefVisitor)?
        };
        let (handle, sender) = get_handle_ref(asset_ref);
        Ok(GenericHandle::new_internal(sender, handle))
    }
}

struct AssetRefVisitor;

impl<'de> Visitor<'de> for AssetRefVisitor {
    type Value = AssetRef;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array of 16 u8")
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        use de::Error;
        let mut uuid: [u8; 16] = Default::default();
        for (i, uuid_byte) in uuid.iter_mut().enumerate() {
            if let Some(byte) = seq.next_element::<u8>()? {
                *uuid_byte = byte;
            } else {
                return Err(A::Error::custom(format!(
                    "expected byte at element {} when deserializing handle",
                    i
                )));
            }
        }
        if seq.next_element::<u8>()?.is_some() {
            return Err(A::Error::custom(
                "too many elements when deserializing handle",
            ));
        }
        Ok(AssetRef::Uuid(AssetUuid(uuid)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        use std::str::FromStr;
        match std::path::PathBuf::from_str(v) {
            Ok(path) => {
                if let Ok(uuid) = uuid::Uuid::parse_str(&path.to_string_lossy()) {
                    Ok(AssetRef::Uuid(AssetUuid(*uuid.as_bytes())))
                } else {
                    Ok(AssetRef::Path(path))
                }
            }
            Err(err) => Err(E::custom(format!(
                "failed to parse Handle string: {:?}",
                err
            ))),
        }
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 16 {
            Err(E::custom(format!(
                "byte array len == {}, expected {}",
                v.len(),
                16
            )))
        } else {
            let mut a = <[u8; 16]>::default();
            a.copy_from_slice(v);
            Ok(AssetRef::Uuid(AssetUuid(a)))
        }
    }
}

/// Implementors of [`crate::storage::AssetStorage`] can implement this trait to enable convenience
/// functions on the common [`AssetHandle`] trait, which is implemented by all handle types.
pub trait TypedAssetStorage<A> {
    /// Returns the asset for the given handle, or `None` if has not completed loading.
    ///
    /// # Parameters
    ///
    /// * `handle`: Handle of the asset.
    ///
    /// # Type Parameters
    ///
    /// * `T`: Asset handle type.
    fn get<T: AssetHandle>(&self, handle: &T) -> Option<&A>;

    /// Returns the version of a loaded asset, or `None` if has not completed loading.
    ///
    /// # Parameters
    ///
    /// * `handle`: Handle of the asset.
    ///
    /// # Type Parameters
    ///
    /// * `T`: Asset handle type.
    fn get_version<T: AssetHandle>(&self, handle: &T) -> Option<u32>;

    /// Returns the loaded asset and its version, or `None` if has not completed loading.
    ///
    /// # Parameters
    ///
    /// * `handle`: Handle of the asset.
    ///
    /// # Type Parameters
    ///
    /// * `T`: Asset handle type.
    fn get_asset_with_version<T: AssetHandle>(&self, handle: &T) -> Option<(&A, u32)>;
}

/// The contract of an asset handle.
///
/// There are two types of asset handles:
///
/// * **Typed -- `Handle<T>`:** When the asset's type is known when loading.
/// * **Generic -- `GenericHandle`:** When only the asset's UUID is known when loading.
pub trait AssetHandle {
    /// Returns the load status of the asset.
    ///
    /// # Parameters
    ///
    /// * `loader`: Loader that is loading the asset.
    ///
    /// # Type Parameters
    ///
    /// * `L`: Asset loader type.
    fn load_status(&self, loader: &Loader) -> LoadStatus {
        loader.get_load_status(self.load_handle())
    }

    /// Returns an immutable reference to the asset if it is committed.
    ///
    /// # Parameters
    ///
    /// * `storage`: Asset storage.
    fn asset<'a, T, S: TypedAssetStorage<T>>(&self, storage: &'a S) -> Option<&'a T>
    where
        Self: Sized,
    {
        storage.get(self)
    }

    /// Returns the version of the asset if it is committed.
    ///
    /// # Parameters
    ///
    /// * `storage`: Asset storage.
    fn asset_version<T, S: TypedAssetStorage<T>>(&self, storage: &S) -> Option<u32>
    where
        Self: Sized,
    {
        storage.get_version(self)
    }

    /// Returns the asset with the given version if it is committed.
    ///
    /// # Parameters
    ///
    /// * `storage`: Asset storage.
    fn asset_with_version<'a, T, S: TypedAssetStorage<T>>(
        &self,
        storage: &'a S,
    ) -> Option<(&'a T, u32)>
    where
        Self: Sized,
    {
        storage.get_asset_with_version(self)
    }

    /// Downgrades this handle into a `WeakHandle`.
    ///
    /// Be aware that if there are no longer any strong handles to the asset, then the underlying
    /// asset may be freed at any time.
    fn downgrade(&self) -> WeakHandle {
        WeakHandle::new(self.load_handle())
    }

    /// Returns the `LoadHandle` of this asset handle.
    fn load_handle(&self) -> LoadHandle;
}
