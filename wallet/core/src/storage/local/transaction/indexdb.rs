use crate::imports::*;
use crate::result::Result;
use crate::storage::interface::StorageStream;
use crate::storage::{Binding, TransactionRecordStore};
use crate::storage::{TransactionMetadata, TransactionRecord};

pub struct Inner {
    known_databases: HashMap<String, HashSet<String>>,
}

pub struct TransactionStore {
    inner: Arc<Mutex<Inner>>,
    name: String,
}

impl TransactionStore {
    pub fn new(name: &str) -> TransactionStore {
        TransactionStore { inner: Arc::new(Mutex::new(Inner { known_databases: HashMap::default() })), name: name.to_string() }
    }

    #[inline(always)]
    fn inner(&self) -> MutexGuard<Inner> {
        self.inner.lock().unwrap()
    }

    pub fn make_db_name(&self, binding: &Binding, network_id: &NetworkId) -> String {
        format!("{}_{}_{}", self.name, binding.to_hex(), network_id)
    }

    pub fn database_is_registered(&self, binding: &str, network_id: &str) -> bool {
        let inner = self.inner();
        if let Some(network_ids) = inner.known_databases.get(binding) {
            network_ids.contains(network_id)
        } else {
            false
        }
    }

    pub fn register_database(&self, binding: &str, network_id: &str) -> Result<()> {
        let mut inner = self.inner();
        if let Some(network_ids) = inner.known_databases.get_mut(binding) {
            network_ids.insert(network_id.to_string());
        } else {
            let mut network_ids = HashSet::new();
            network_ids.insert(network_id.to_string());
            inner.known_databases.insert(binding.to_string(), network_ids);
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn ensure_database(&self, binding: &Binding, network_id: &NetworkId) -> Result<()> {
        let binding_hex = binding.to_hex();
        let network_id = network_id.to_string();
        if !self.database_is_registered(&binding_hex, &network_id) {
            // - TODO
            self.register_database(&binding_hex, &network_id)?;
        }
        Ok(())
    }

    pub async fn store_transaction_metadata(&self, _id: TransactionId, _metadata: TransactionMetadata) -> Result<()> {
        Ok(())
    }

    async fn init_or_get_db(&self, binding: &Binding, network_id: &NetworkId) -> Result<idb::Database> {
        // Get a factory instance from global scope
        let factory = idb::Factory::new().map_err(|err| format!("Error creating indexed db factory: {}", err))?;

        let db_name = self.make_db_name(binding, network_id);

        // Create an open request for the database
        let mut open_request = factory.open(&db_name, Some(1)).unwrap();

        // Add an upgrade handler for database
        open_request.on_upgrade_needed(|event| {
            // Get database instance from event
            let database = event.database().unwrap();

            // Prepare object store params
            let mut store_params = idb::ObjectStoreParams::new();
            store_params.key_path(Some(idb::KeyPath::new_single("id")));

            // Create object store
            let store = database.create_object_store("transactions", store_params).unwrap();

            // version: u16,
            // id: TransactionId,
            // #[serde(skip_serializing_if = "Option::is_none")]
            // unixtime: Option<u64>,
            // binding: Binding,
            // #[serde(rename = "blockDaaScore")]
            // block_daa_score: u64,
            // #[serde(rename = "network")]
            // network_id: NetworkId,
            // #[serde(rename = "data")]
            // transaction_data: TransactionData,
            // #[serde(skip_serializing_if = "Option::is_none")]
            // metadata: Option<TransactionMetadata>,

            const IDB_VERSION_INDEX: &str = "version";
            const IDB_TIMESTAMP_INDEX: &str = "timestamp";
            const IDB_BLOCK_DAA_SCORE_INDEX: &str = "blockDaaScore";
            const IDB_TRANSACTION_DATA_INDEX: &str = "data";
            const IDB_TRANSACTION_METADATA_INDEX: &str = "metadata";

            store.create_index(IDB_VERSION_INDEX, idb::KeyPath::new_single(IDB_VERSION_INDEX), None).unwrap();
            store.create_index(IDB_TIMESTAMP_INDEX, idb::KeyPath::new_single(IDB_TIMESTAMP_INDEX), None).unwrap();
            store.create_index(IDB_BLOCK_DAA_SCORE_INDEX, idb::KeyPath::new_single(IDB_BLOCK_DAA_SCORE_INDEX), None).unwrap();
            store.create_index(IDB_TRANSACTION_DATA_INDEX, idb::KeyPath::new_single(IDB_TRANSACTION_DATA_INDEX), None).unwrap();
            store
                .create_index(IDB_TRANSACTION_METADATA_INDEX, idb::KeyPath::new_single(IDB_TRANSACTION_METADATA_INDEX), None)
                .unwrap();
        });

        // `await` open request
        let db = open_request.await.map_err(|err| Error::Custom(format!("Error opening database: {}", err)))?;
        todo!()
        //
        // Ok(db)
    }
}

#[async_trait]
impl TransactionRecordStore for TransactionStore {
    async fn transaction_id_iter(&self, binding: &Binding, network_id: &NetworkId) -> Result<StorageStream<Arc<TransactionId>>> {
        Ok(Box::pin(TransactionIdStream::try_new(self, binding, network_id).await?))
    }

    // async fn transaction_iter(&self, binding: &Binding, network_id: &NetworkId) -> Result<StorageStream<TransactionRecord>> {
    //     Ok(Box::pin(TransactionRecordStream::try_new(&self.transactions, binding, network_id).await?))
    // }

    async fn load_single(&self, binding: &Binding, network_id: &NetworkId, _id: &TransactionId) -> Result<Arc<TransactionRecord>> {
        let db = self.init_or_get_db(binding, network_id).await?;

        Err(Error::NotImplemented)
    }

    async fn load_multiple(
        &self,
        _binding: &Binding,
        _network_id: &NetworkId,
        _ids: &[TransactionId],
    ) -> Result<Vec<Arc<TransactionRecord>>> {
        Ok(vec![])
    }

    async fn store(&self, _transaction_records: &[&TransactionRecord]) -> Result<()> {
        Ok(())
    }

    async fn remove(&self, _binding: &Binding, _network_id: &NetworkId, _ids: &[&TransactionId]) -> Result<()> {
        Ok(())
    }

    async fn store_transaction_metadata(&self, _id: TransactionId, _metadata: TransactionMetadata) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct TransactionIdStream {}

impl TransactionIdStream {
    pub(crate) async fn try_new(_store: &TransactionStore, _binding: &Binding, _network_id: &NetworkId) -> Result<Self> {
        Ok(Self {})
    }
}

impl Stream for TransactionIdStream {
    type Item = Result<Arc<TransactionId>>;

    #[allow(unused_mut)]
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
