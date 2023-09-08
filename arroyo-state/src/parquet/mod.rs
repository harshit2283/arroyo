use crate::parquet::writer::{ParquetFileWriter, ParquetWriter};
use crate::tables::{BlindTuple, MemoryTableType};
use crate::{hash_key, BackingStore, DataOperation, StateStore, BINCODE_CONFIG};
use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arroyo_rpc::grpc::backend_data::BackendData;
use arroyo_rpc::grpc::worker_grpc_client::WorkerGrpcClient;
use arroyo_rpc::grpc::{
    backend_data, CheckpointMetadata, LoadCompactedDataReq, OperatorCheckpointMetadata,
    ParquetStoreData, TableDescriptor, TableType, WorkerStatus,
};
use arroyo_rpc::ControlResp;
use arroyo_storage::StorageProvider;
use arroyo_types::{
    from_micros, range_for_server, CheckpointBarrier, Data, Key, TaskInfo, WorkerId,
    CHECKPOINT_URL_ENV, S3_ENDPOINT_ENV, S3_REGION_ENV,
};
use bincode::config;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::ops::RangeInclusive;
use std::time::SystemTime;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

mod writer;

async fn get_storage_provider() -> anyhow::Result<StorageProvider> {
    // TODO: this should be encoded in the config so that the controller doesn't need
    // to be synchronized with the workers
    let storage_url =
        env::var(CHECKPOINT_URL_ENV).unwrap_or_else(|_| "file:///tmp/arroyo".to_string());

    StorageProvider::for_url(&storage_url)
        .await
        .context(format!(
            "failed to construct checkpoint backend for URL {}",
            storage_url
        ))
}

pub struct ParquetBackend {
    epoch: u32,
    min_epoch: u32,
    // ordered by table, then epoch.
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    writer: ParquetWriter,
    task_info: TaskInfo,
    tables: HashMap<char, TableDescriptor>,
    storage: StorageProvider,
}

fn base_path(job_id: &str, epoch: u32) -> String {
    format!("{}/checkpoints/checkpoint-{:0>7}", job_id, epoch)
}

fn metadata_path(path: &str) -> String {
    format!("{}/metadata", path)
}

fn operator_path(job_id: &str, epoch: u32, operator: &str) -> String {
    format!("{}/operator-{}", base_path(job_id, epoch), operator)
}

fn table_checkpoint_path(task_info: &TaskInfo, table: char, epoch: u32, compacted: bool) -> String {
    format!(
        "{}/table-{}-{:0>3}{}",
        operator_path(&task_info.job_id, epoch, &task_info.operator_id),
        table,
        task_info.task_index,
        if compacted { "-compacted" } else { "" }
    )
}

struct CompactionResult {
    operator_id: String,
    backend_data_to_drop: Vec<arroyo_rpc::grpc::BackendData>,
    backend_data_to_load: Vec<arroyo_rpc::grpc::BackendData>,
}

#[async_trait::async_trait]
impl BackingStore for ParquetBackend {
    fn name() -> &'static str {
        "parquet"
    }

    fn task_info(&self) -> &TaskInfo {
        &self.task_info
    }

    async fn load_latest_checkpoint_metadata(_job_id: &str) -> Option<CheckpointMetadata> {
        todo!()
    }

    // TODO: should this be a Result, rather than an option?
    async fn load_checkpoint_metadata(job_id: &str, epoch: u32) -> Option<CheckpointMetadata> {
        let storage_client = get_storage_provider().await.unwrap();
        let data = storage_client
            .get(&metadata_path(&base_path(job_id, epoch)))
            .await
            .ok()?;
        let metadata = CheckpointMetadata::decode(&data[..]).unwrap();
        Some(metadata)
    }

    async fn load_operator_metadata(
        job_id: &str,
        operator_id: &str,
        epoch: u32,
    ) -> Option<OperatorCheckpointMetadata> {
        let storage_client = get_storage_provider().await.unwrap();
        let data = storage_client
            .get(&metadata_path(&operator_path(job_id, epoch, operator_id)))
            .await
            .ok()?;
        Some(OperatorCheckpointMetadata::decode(&data[..]).unwrap())
    }

    async fn write_operator_checkpoint_metadata(metadata: OperatorCheckpointMetadata) {
        let storage_client = get_storage_provider().await.unwrap();
        let path = metadata_path(&operator_path(
            &metadata.job_id,
            metadata.epoch,
            &metadata.operator_id,
        ));
        // TODO: propagate error
        storage_client
            .put(&path, metadata.encode_to_vec())
            .await
            .unwrap();
    }

    async fn complete_checkpoint(metadata: CheckpointMetadata) {
        debug!("writing checkpoint {:?}", metadata);
        let storage_client = get_storage_provider().await.unwrap();
        let path = metadata_path(&base_path(&metadata.job_id, metadata.epoch));
        // TODO: propagate error
        storage_client
            .put(&path, metadata.encode_to_vec())
            .await
            .unwrap();
    }

    async fn new(
        task_info: &TaskInfo,
        tables: Vec<TableDescriptor>,
        tx: Sender<ControlResp>,
    ) -> Self {
        let storage = get_storage_provider().await.unwrap();
        Self {
            epoch: 1,
            min_epoch: 1,
            current_files: HashMap::new(),
            writer: ParquetWriter::new(
                task_info.clone(),
                tx,
                tables.clone(),
                storage.clone(),
                HashMap::new(),
            ),
            task_info: task_info.clone(),
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone().chars().next().unwrap(), table))
                .collect(),
            storage,
        }
    }

    async fn from_checkpoint(
        task_info: &TaskInfo,
        metadata: CheckpointMetadata,
        tables: Vec<TableDescriptor>,
        control_tx: Sender<ControlResp>,
    ) -> Self {
        let operator_metadata =
            Self::load_operator_metadata(&task_info.job_id, &task_info.operator_id, metadata.epoch)
                .await
                .unwrap_or_else(|| {
                    panic!(
                        "missing metadata for operator {}, epoch {}",
                        task_info.operator_id, metadata.epoch
                    )
                });

        let mut current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> = HashMap::new();
        let tables: HashMap<char, TableDescriptor> = tables
            .into_iter()
            .map(|table| (table.name.clone().chars().next().unwrap(), table))
            .collect();
        for backend_data in operator_metadata.backend_data {
            let Some(backend_data::BackendData::ParquetStore(parquet_data)) =
                backend_data.backend_data
            else {
                panic!("expect parquet data")
            };
            let table_descriptor = tables
                .get(&parquet_data.table.chars().next().unwrap())
                .unwrap();
            if table_descriptor.table_type() != TableType::Global {
                // check if the file has data in the task's key range.
                if parquet_data.max_routing_key < *task_info.key_range.start()
                    || *task_info.key_range.end() < parquet_data.min_routing_key
                {
                    continue;
                }
            }

            let files = current_files
                .entry(parquet_data.table.chars().next().unwrap())
                .or_default()
                .entry(parquet_data.epoch)
                .or_default();
            files.push(parquet_data);
        }

        let writer_current_files = current_files.clone();

        let storage = get_storage_provider().await.unwrap();

        Self {
            epoch: metadata.epoch + 1,
            min_epoch: metadata.min_epoch,
            current_files,
            writer: ParquetWriter::new(
                task_info.clone(),
                control_tx,
                tables.values().cloned().collect(),
                storage.clone(),
                writer_current_files,
            ),
            task_info: task_info.clone(),
            tables,
            storage,
        }
    }

    async fn prepare_checkpoint_load(_metadata: &CheckpointMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    async fn compact_checkpoint(
        mut metadata: CheckpointMetadata,
        old_min_epoch: u32,
        min_epoch: u32,
        worker_clients: Vec<WorkerGrpcClient<Channel>>,
    ) -> Result<()> {
        info!(message = "Compacting", min_epoch, job_id = metadata.job_id);

        let mut futures: FuturesUnordered<_> = metadata
            .operator_ids
            .iter()
            .map(|operator_id| {
                Self::compact_operator(
                    metadata.job_id.clone(),
                    operator_id.clone(),
                    old_min_epoch,
                    min_epoch,
                )
            })
            .collect();

        let storage_client = Mutex::new(get_storage_provider().await?);

        let mut compaction_results = vec![];
        // wait for all of the futures to complete
        while let Some(result) = futures.next().await {
            let compaction_result = result?;

            for epoch_to_remove in old_min_epoch..min_epoch {
                let path = metadata_path(&operator_path(
                    &metadata.job_id,
                    epoch_to_remove,
                    &compaction_result.operator_id,
                ));
                storage_client.lock().await.delete(path).await?;
            }
            debug!(
                message = "Finished compacting operator",
                job_id = metadata.job_id,
                compaction_result.operator_id,
                min_epoch
            );
            compaction_results.push(compaction_result);
        }

        for mut worker_client in worker_clients {
            for compaction_result in compaction_results.iter() {
                worker_client
                    .load_compacted_data(LoadCompactedDataReq {
                        operator_id: compaction_result.operator_id.clone(),
                        backend_data_to_drop: compaction_result.backend_data_to_drop.clone(),
                        backend_data_to_load: compaction_result.backend_data_to_load.clone(),
                    })
                    .await?;
            }
        }

        for epoch_to_remove in old_min_epoch..min_epoch {
            storage_client
                .lock()
                .await
                .delete(metadata_path(&base_path(&metadata.job_id, epoch_to_remove)))
                .await?;
        }
        metadata.min_epoch = min_epoch;
        Self::complete_checkpoint(metadata).await;
        Ok(())
    }

    async fn checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        watermark: Option<SystemTime>,
    ) -> u32 {
        assert_eq!(barrier.epoch, self.epoch);
        self.writer
            .checkpoint(self.epoch, barrier.timestamp, watermark, barrier.then_stop)
            .await;
        self.epoch += 1;
        self.min_epoch = barrier.min_epoch;
        self.epoch - 1
    }

    async fn get_data_tuples<K: Key, V: Data>(
        &self,
        table: char,
    ) -> Vec<(SystemTime, K, V, DataOperation)> {
        let mut result = vec![];
        match self.tables.get(&table).unwrap().table_type() {
            TableType::Global => todo!(),
            TableType::TimeKeyMap | TableType::KeyTimeMultiMap => {
                // current_files is  table -> epoch -> file
                // so we look at all epoch's files for this table
                let Some(files) = self.current_files.get(&table) else {
                    return vec![];
                };
                for file in files.values().flatten() {
                    let bytes = self.storage.get(&file.file).await.unwrap_or_else(|_| {
                        panic!("unable to find file {} in checkpoint", file.file)
                    });
                    result.append(
                        &mut self
                            .tuples_from_parquet_bytes(bytes.into(), &self.task_info.key_range),
                    );
                }
            }
        }
        result
    }

    async fn write_data_tuple<K: Key, V: Data>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
        value: &mut V,
    ) {
        let (key_hash, key_bytes, value_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
                bincode::encode_to_vec(&*value, config::standard()).unwrap(),
            )
        };
        self.writer
            .write(
                table,
                key_hash,
                timestamp,
                key_bytes,
                value_bytes,
                DataOperation::Insert,
            )
            .await;
    }

    async fn delete_data_tuple<K: Key>(
        &mut self,
        table: char,
        _table_type: TableType,
        timestamp: SystemTime,
        key: &mut K,
    ) {
        let (key_hash, key_bytes) = {
            (
                hash_key(key),
                bincode::encode_to_vec(&*key, config::standard()).unwrap(),
            )
        };
        self.writer
            .write(
                table,
                key_hash,
                timestamp,
                key_bytes,
                vec![],
                DataOperation::DeleteKey,
            )
            .await;
    }

    async fn write_key_value<K: Key, V: Data>(&mut self, table: char, key: &mut K, value: &mut V) {
        self.write_data_tuple(table, TableType::Global, SystemTime::UNIX_EPOCH, key, value)
            .await
    }

    async fn delete_key_value<K: Key>(&mut self, table: char, key: &mut K) {
        self.delete_data_tuple(table, TableType::Global, SystemTime::UNIX_EPOCH, key)
            .await
    }

    /// Get all key-value pairs in the given table.
    /// Looks at the operation to determine if the key-value pair should be included.
    async fn get_key_values<K: Key, V: Data>(
        &self,
        table: char,
        key_range: &RangeInclusive<u64>,
    ) -> Vec<(K, V)> {
        let Some(files) = self.current_files.get(&table) else {
            return vec![];
        };
        let mut state_map = HashMap::new();
        for file in files.values().flatten() {
            let bytes = self
                .storage
                .get(&file.file)
                .await
                .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file))
                .into();
            for (_timestamp, key, value, operation) in
                self.tuples_from_parquet_bytes(bytes, key_range)
            {
                match operation {
                    DataOperation::Insert => {
                        state_map.insert(key, value);
                    }
                    DataOperation::DeleteKey => {
                        state_map.remove(&key);
                    }
                }
            }
        }
        state_map.into_iter().collect()
    }

    async fn load_compacted_files(
        &mut self,
        backend_data_to_drop: Vec<arroyo_rpc::grpc::BackendData>,
        backend_data_to_load: Vec<arroyo_rpc::grpc::BackendData>,
    ) {
        info!("task info: {:?}", self.task_info);
        info!("current tables: {:?}", self.current_files.keys());

        // add all the new files
        for backend_data in backend_data_to_load {
            let Some(BackendData::ParquetStore(parquet_store)) = backend_data.backend_data else {
                unreachable!("expect parquet backends")
            };
            let table_char = parquet_store.table.chars().next().unwrap();
            let epoch = parquet_store.epoch;

            if !self.current_files.contains_key(&table_char) {
                warn!(
                    "Received compaction data for table {} but this task does not have that table",
                    table_char
                );
                continue;
            }

            if !self
                .current_files
                .get(&table_char)
                .unwrap()
                .contains_key(&epoch)
            {
                warn!(
                        "Received compaction data for table {} epoch {} but this task does not have that epoch",
                        table_char,
                        epoch
                    );
                continue;
            }

            info!("Loading compacted file {:?}", parquet_store.file);

            // add to current_files
            self.current_files
                .get_mut(&table_char)
                .unwrap()
                .get_mut(&epoch)
                .unwrap()
                .push(parquet_store);
        }

        // remove all the old files
        for backend_data in backend_data_to_drop {
            let Some(BackendData::ParquetStore(parquet_store)) = backend_data.backend_data else {
                unreachable!("expect parquet backends")
            };
            let table_char = parquet_store.table.chars().next().unwrap();
            let epoch = parquet_store.epoch;

            if !self.current_files.contains_key(&table_char) {
                warn!(
                    "Received compaction data for table {} but this task does not have that table",
                    table_char
                );
                continue;
            }

            if !self
                .current_files
                .get(&table_char)
                .unwrap()
                .contains_key(&epoch)
            {
                warn!(
                        "Received compaction data for table {} epoch {} but this task does not have that epoch",
                        table_char,
                        epoch
                    );
                continue;
            }

            let files = self
                .current_files
                .get_mut(&table_char)
                .unwrap()
                .get_mut(&epoch)
                .unwrap();

            // remove the file from the list of files for this epoch
            files.remove(
                files
                    .iter()
                    .position(|file| file.file == parquet_store.file)
                    .unwrap(),
            );
        }
    }
}

impl ParquetBackend {
    async fn compact_table_partition(
        table_char: char,
        task: TaskInfo,
        epoch_files: BTreeMap<u32, Vec<ParquetStoreData>>,
        storage_client: StorageProvider,
        new_min_epoch: u32,
        table_descriptor: &TableDescriptor,
    ) -> Option<ParquetStoreData> {
        // accumulate this partition's tuples from all the table's files
        // (spread across multiple epochs and files)
        let mut tuples_in = vec![];
        for (_epoch, files) in epoch_files {
            for file in files {
                let bytes = storage_client
                    .get(&file.file)
                    .await
                    .unwrap_or_else(|_| panic!("unable to find file {} in checkpoint", file.file));
                tuples_in.extend(ParquetBackend::blind_tuples_from_parquet_bytes(
                    bytes.into(),
                    &task.key_range,
                ));
            }
        }

        // do the compaction
        let m: MemoryTableType = table_descriptor.table_type().into();
        let tuples_length = tuples_in.len();
        let tuples_out = m.compact_tuples(tuples_in);

        if tuples_length != tuples_out.len() {
            info!(
                "compaction summary for operator {}, table {}, task {}: {} tuples in, {} tuples out, {} tuples compacted",
                task.operator_id,
                table_char,
                task.task_index,
                tuples_length,
                tuples_out.len(),
                tuples_length - tuples_out.len()
            );
        }

        let mut parquet_writer = ParquetFileWriter::new(table_char, new_min_epoch, task.clone());

        for tuple in tuples_out {
            parquet_writer.write(
                tuple.key_hash,
                tuple.timestamp,
                tuple.key,
                tuple.value,
                tuple.operation,
            );
        }

        let p = parquet_writer
            .flush(&storage_client)
            .await
            .expect("Failed to flush parquet writer");
        p
    }

    async fn compact_operator(
        job_id: String,
        operator_id: String,
        old_min_epoch: u32,
        new_min_epoch: u32,
    ) -> anyhow::Result<CompactionResult> {
        let operator_checkpoint_metadata =
            Self::load_operator_metadata(&job_id, &operator_id, new_min_epoch)
                .await
                .expect("expect new_min_epoch metadata to still be present");

        let backend_data_to_drop: Vec<arroyo_rpc::grpc::BackendData> =
            operator_checkpoint_metadata.backend_data;

        let paths_to_keep: HashSet<String> = backend_data_to_drop
            .iter()
            .map(|backend_data| {
                let Some(BackendData::ParquetStore(parquet_store)) = &backend_data.backend_data
                else {
                    unreachable!("expect parquet backends")
                };
                parquet_store.file.clone()
            })
            .collect();

        let mut deleted_paths = HashSet::new();
        let storage_client = get_storage_provider().await?;

        // clean up files from old epochs
        for epoch_to_remove in old_min_epoch..new_min_epoch {
            let Some(metadata) =
                Self::load_operator_metadata(&job_id, &operator_id, epoch_to_remove).await
            else {
                continue;
            };

            // delete any files that are not in the new min epoch
            for backend_data in metadata.backend_data {
                let Some(BackendData::ParquetStore(parquet_store)) = &backend_data.backend_data
                else {
                    unreachable!("expect parquet backends")
                };
                let file = parquet_store.file.clone();
                if !paths_to_keep.contains(&file) && !deleted_paths.contains(&file) {
                    deleted_paths.insert(file.clone());
                    storage_client.delete(file.clone()).await?;
                    info!("deleted {}", file);
                }
            }
        }

        let checkpoint_metadata = Self::load_checkpoint_metadata(&job_id, new_min_epoch)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "missing checkpoint metadata for job {}, epoch {}",
                    job_id, new_min_epoch
                )
            });

        let mut backend_data_to_load = vec![]; // one file per partition per table

        // we reduce the range of epochs to 16-partitioned compacted files
        // (16 files per table)
        let parallelism = 16; // TODO: set based on parallelism
        for index in 1..parallelism {
            let key_range = range_for_server(index, parallelism);
            let task = TaskInfo {
                job_id: job_id.clone(),
                operator_name: "".to_string(), // TODO
                operator_id: operator_id.clone(),
                task_index: index,
                parallelism,
                key_range: key_range.clone(),
            };
            let (tx, _) = channel(10);
            let mut state_store = StateStore::<ParquetBackend>::from_checkpoint(
                &task,
                checkpoint_metadata.clone(),
                operator_checkpoint_metadata.tables.clone(),
                tx.clone(), // TODO
            )
            .await;

            // for each table this operator has, generate this partition's compacted file
            for (table_char, epoch_files) in state_store.backend.current_files.drain() {
                let p = ParquetBackend::compact_table_partition(
                    table_char,
                    task.clone(),
                    epoch_files,
                    storage_client.clone(),
                    new_min_epoch,
                    state_store.table_descriptors.get(&table_char).unwrap(),
                )
                .await;

                if let Some(p) = p {
                    backend_data_to_load.push(arroyo_rpc::grpc::BackendData {
                        backend_data: Some(BackendData::ParquetStore(p)),
                    });
                }
            }
        }

        // Notify the workers that there is compressed data to load

        Ok(CompactionResult {
            operator_id,
            backend_data_to_drop,
            backend_data_to_load,
        })
    }

    /// Return rows from the given bytes that are in the given key range
    fn tuples_from_parquet_bytes<K: Key, V: Data>(
        &self,
        bytes: Vec<u8>,
        range: &RangeInclusive<u64>,
    ) -> Vec<(SystemTime, K, V, DataOperation)> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes))
            .unwrap()
            .build()
            .unwrap();

        let mut result = vec![];

        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        for batch in batches {
            let num_rows = batch.num_rows();
            let key_hash_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let time_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .expect("Column 1 is not a TimestampMicrosecondArray");
            let key_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let value_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let operation_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .unwrap();
            for index in 0..num_rows {
                if !range.contains(&key_hash_array.value(index)) {
                    continue;
                }

                let timestamp = from_micros(time_array.value(index) as u64);

                let key: K = bincode::decode_from_slice(key_array.value(index), BINCODE_CONFIG)
                    .unwrap()
                    .0;
                let value: V = bincode::decode_from_slice(value_array.value(index), BINCODE_CONFIG)
                    .unwrap()
                    .0;
                let operation = operation_array.value(index).into();

                result.push((timestamp, key, value, operation));
            }
        }
        result
    }

    /// Return rows from the given bytes that are in the given key range,
    /// but without deserializing the key and value.
    fn blind_tuples_from_parquet_bytes(
        bytes: Vec<u8>,
        range: &RangeInclusive<u64>,
    ) -> Vec<BlindTuple> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes))
            .unwrap()
            .build()
            .unwrap();

        let mut result = vec![];

        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        for batch in batches {
            let num_rows = batch.num_rows();
            let key_hash_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap();
            let time_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .expect("Column 1 is not a TimestampMicrosecondArray");
            let key_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let value_array = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .unwrap();
            let operation_array = batch
                .column(4)
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .unwrap();
            for index in 0..num_rows {
                let key_hash = key_hash_array.value(index);
                if !range.contains(&key_hash) {
                    continue;
                }

                let timestamp = from_micros(time_array.value(index) as u64);

                let key = key_array.value(index).to_owned();
                let value = value_array.value(index).to_owned();
                let operation = operation_array.value(index).into();

                result.push(BlindTuple {
                    key_hash,
                    timestamp,
                    key,
                    value,
                    operation,
                });
            }
        }
        result
    }
}

pub fn get_storage_env_vars() -> HashMap<String, String> {
    [S3_REGION_ENV, S3_ENDPOINT_ENV, CHECKPOINT_URL_ENV]
        .iter()
        .filter_map(|&var| env::var(var).ok().map(|v| (var.to_string(), v)))
        .collect()
}
