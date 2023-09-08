use crate::parquet::table_checkpoint_path;
use crate::DataOperation;
use anyhow::Result;
use arroyo_rpc::grpc::backend_data::BackendData;
use arroyo_rpc::grpc::{
    ParquetStoreData, SubtaskCheckpointMetadata, TableDeleteBehavior, TableDescriptor, TableType,
};
use arroyo_rpc::{CheckpointCompleted, ControlResp};
use arroyo_storage::StorageProvider;
use arroyo_types::{to_micros, TaskInfo};
use parquet::arrow::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

/// A ParquetFileWriter accumulates writes to a single parquet file
pub struct ParquetFileWriter {
    table_char: char,
    epoch: u32,
    task_info: TaskInfo,
    builder: RecordBatchBuilder,
}

impl ParquetFileWriter {
    pub fn new(table_char: char, epoch: u32, task_info: TaskInfo) -> Self {
        ParquetFileWriter {
            table_char,
            epoch,
            task_info,
            builder: RecordBatchBuilder::default(),
        }
    }

    pub(crate) fn write(
        &mut self,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.builder
            .insert(key_hash, timestamp, key, data, operation);
    }

    async fn upload_record_batch(
        key: &str,
        record_batch: arrow_array::RecordBatch,
        storage: &StorageProvider,
    ) -> Result<usize> {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch).expect("Writing batch");
        writer.flush().unwrap();
        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len();
        storage.put(key, parquet_bytes).await?;
        Ok(bytes)
    }

    pub async fn flush(self, storage: &StorageProvider) -> Result<Option<ParquetStoreData>> {
        let s3_key = table_checkpoint_path(&self.task_info, self.table_char, self.epoch, true);
        match self.builder.flush() {
            Some((record_batch, stats)) => {
                ParquetFileWriter::upload_record_batch(&s3_key, record_batch, storage).await?;
                return Ok(Some(ParquetStoreData {
                    epoch: self.epoch,
                    file: s3_key,
                    table: self.table_char.to_string(),
                    min_routing_key: stats.min_routing_key,
                    max_routing_key: stats.max_routing_key,
                    max_timestamp_micros: to_micros(stats.max_timestamp),
                    min_required_timestamp_micros: None,
                }));
            }
            None => Ok(None),
        }
    }
}

pub struct ParquetWriter {
    sender: Sender<ParquetQueueItem>,
    finish_rx: Option<oneshot::Receiver<()>>,
}

impl ParquetWriter {
    pub(crate) fn new(
        task_info: TaskInfo,
        control_tx: Sender<ControlResp>,
        tables: Vec<TableDescriptor>,
        storage: StorageProvider,
        current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(1024 * 1024);
        let (finish_tx, finish_rx) = oneshot::channel();

        (ParquetFlusher {
            queue: rx,
            storage,
            control_tx,
            finish_tx: Some(finish_tx),
            task_info,
            table_descriptors: tables
                .iter()
                .map(|table| (table.name.chars().next().unwrap(), table.clone()))
                .collect(),
            builders: HashMap::new(),
            current_files,
        })
        .start();

        ParquetWriter {
            sender: tx,
            finish_rx: Some(finish_rx),
        }
    }

    pub(crate) async fn write(
        &mut self,
        table: char,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.sender
            .send(ParquetQueueItem::Write(ParquetWrite {
                table,
                key_hash,
                timestamp,
                key,
                data,
                operation,
            }))
            .await
            .unwrap();
    }

    pub(crate) async fn checkpoint(
        &mut self,
        epoch: u32,
        time: SystemTime,
        watermark: Option<SystemTime>,
        then_stop: bool,
    ) {
        self.sender
            .send(ParquetQueueItem::Checkpoint(ParquetCheckpoint {
                epoch,
                time,
                watermark,
                then_stop,
            }))
            .await
            .unwrap();
        if then_stop {
            match self.finish_rx.take().unwrap().await {
                Ok(_) => info!("finished stopping checkpoint"),
                Err(err) => warn!("error waiting for stopping checkpoint {:?}", err),
            }
        }
    }
}

#[derive(Debug)]
enum ParquetQueueItem {
    Write(ParquetWrite),
    Checkpoint(ParquetCheckpoint),
}

#[derive(Debug)]
struct ParquetWrite {
    table: char,
    key_hash: u64,
    timestamp: SystemTime,
    key: Vec<u8>,
    data: Vec<u8>,
    operation: DataOperation,
}

#[derive(Debug)]
struct ParquetCheckpoint {
    epoch: u32,
    time: SystemTime,
    watermark: Option<SystemTime>,
    then_stop: bool,
}

struct RecordBatchBuilder {
    key_hash_builder: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt64Type>,
    start_time_array:
        arrow_array::builder::PrimitiveBuilder<arrow_array::types::TimestampMicrosecondType>,
    key_bytes: arrow_array::builder::BinaryBuilder,
    data_bytes: arrow_array::builder::BinaryBuilder,
    parquet_stats: ParquetStats,
    operation_array: arrow_array::builder::PrimitiveBuilder<arrow_array::types::UInt32Type>,
}

struct ParquetStats {
    max_timestamp: SystemTime,
    min_routing_key: u64,
    max_routing_key: u64,
}

impl Default for ParquetStats {
    fn default() -> Self {
        Self {
            max_timestamp: SystemTime::UNIX_EPOCH,
            min_routing_key: u64::MAX,
            max_routing_key: u64::MIN,
        }
    }
}

impl RecordBatchBuilder {
    fn insert(
        &mut self,
        key_hash: u64,
        timestamp: SystemTime,
        key: Vec<u8>,
        data: Vec<u8>,
        operation: DataOperation,
    ) {
        self.parquet_stats.min_routing_key = self.parquet_stats.min_routing_key.min(key_hash);
        self.parquet_stats.max_routing_key = self.parquet_stats.max_routing_key.max(key_hash);

        self.key_hash_builder.append_value(key_hash);
        self.start_time_array
            .append_value(to_micros(timestamp) as i64);
        self.key_bytes.append_value(key);
        self.data_bytes.append_value(data);

        let o = operation as u32;

        self.operation_array.append_value(o);
        self.parquet_stats.max_timestamp = self.parquet_stats.max_timestamp.max(timestamp);
    }

    fn flush(mut self) -> Option<(arrow_array::RecordBatch, ParquetStats)> {
        let key_hash_array: arrow_array::PrimitiveArray<arrow_array::types::UInt64Type> =
            self.key_hash_builder.finish();
        if key_hash_array.is_empty() {
            return None;
        }
        let start_time_array: arrow_array::PrimitiveArray<
            arrow_array::types::TimestampMicrosecondType,
        > = self.start_time_array.finish();
        let key_array: arrow_array::BinaryArray = self.key_bytes.finish();
        let data_array: arrow_array::BinaryArray = self.data_bytes.finish();
        let operations_array = self.operation_array.finish();
        Some((
            arrow_array::RecordBatch::try_new(
                self.schema(),
                vec![
                    std::sync::Arc::new(key_hash_array),
                    std::sync::Arc::new(start_time_array),
                    std::sync::Arc::new(key_array),
                    std::sync::Arc::new(data_array),
                    std::sync::Arc::new(operations_array),
                ],
            )
            .unwrap(),
            self.parquet_stats,
        ))
    }

    fn schema(&self) -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("key_hash", arrow::datatypes::DataType::UInt64, false),
            arrow::datatypes::Field::new(
                "start_time",
                arrow::datatypes::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
            arrow::datatypes::Field::new("key_bytes", arrow::datatypes::DataType::Binary, false),
            arrow::datatypes::Field::new(
                "aggregate_bytes",
                arrow::datatypes::DataType::Binary,
                false,
            ),
            arrow::datatypes::Field::new("operation", arrow::datatypes::DataType::UInt32, false),
        ]))
    }
}
impl Default for RecordBatchBuilder {
    fn default() -> Self {
        Self {
            key_hash_builder: arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::UInt64Type,
            >::with_capacity(1024),
            start_time_array: arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::TimestampMicrosecondType,
            >::with_capacity(1024),
            key_bytes: arrow_array::builder::BinaryBuilder::default(),
            data_bytes: arrow_array::builder::BinaryBuilder::default(),
            operation_array: arrow_array::builder::PrimitiveBuilder::<
                arrow_array::types::UInt32Type,
            >::with_capacity(1024),
            parquet_stats: ParquetStats::default(),
        }
    }
}

struct ParquetFlusher {
    queue: Receiver<ParquetQueueItem>,
    storage: StorageProvider,
    control_tx: Sender<ControlResp>,
    finish_tx: Option<oneshot::Sender<()>>,
    task_info: TaskInfo,
    table_descriptors: HashMap<char, TableDescriptor>,
    builders: HashMap<char, RecordBatchBuilder>,
    current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>>, // table -> epoch -> file
}

impl ParquetFlusher {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                if !self.flush_iteration().await.unwrap() {
                    return;
                }
            }
        });
    }
    async fn upload_record_batch(
        &self,
        key: &str,
        record_batch: arrow_array::RecordBatch,
    ) -> Result<usize> {
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let cursor = Vec::new();
        let mut writer = ArrowWriter::try_new(cursor, record_batch.schema(), Some(props)).unwrap();
        writer.write(&record_batch).expect("Writing batch");
        writer.flush().unwrap();
        let parquet_bytes = writer.into_inner().unwrap();
        let bytes = parquet_bytes.len();
        self.storage.put(key, parquet_bytes).await?;
        Ok(bytes)
    }

    async fn flush_iteration(&mut self) -> Result<bool> {
        let mut checkpoint_epoch = None;

        // accumulate writes in the RecordBatchBuilders until we get a checkpoint
        while checkpoint_epoch.is_none() {
            tokio::select! {
                op = self.queue.recv() => {
                    match op {
                        Some(ParquetQueueItem::Write( ParquetWrite{table, key_hash, timestamp, key, data, operation})) => {
                            self.builders.entry(table).or_default().insert(key_hash, timestamp, key, data, operation);
                        }
                        Some(ParquetQueueItem::Checkpoint(epoch)) => {
                            checkpoint_epoch = Some(epoch);
                        },
                        None => {
                            debug!("Parquet flusher closed");
                            return Ok(false);
                        }
                    }
                }
            }
        }

        if let Some(cp) = checkpoint_epoch {
            let mut bytes = 0;
            let mut to_write = vec![];
            for (table, builder) in self.builders.drain() {
                let Some((record_batch, stats)) = builder.flush() else {
                    continue;
                };
                let s3_key = table_checkpoint_path(&self.task_info, table, cp.epoch, false);
                to_write.push((record_batch, s3_key, table, stats));
            }

            // write the files and update current_files
            for (record_batch, s3_key, table, stats) in to_write {
                bytes += self.upload_record_batch(&s3_key, record_batch).await?;
                self.current_files
                    .entry(table)
                    .or_default()
                    .entry(cp.epoch)
                    .or_default()
                    .push(ParquetStoreData {
                        epoch: cp.epoch,
                        file: s3_key,
                        table: table.to_string(),
                        min_routing_key: stats.min_routing_key,
                        max_routing_key: stats.max_routing_key,
                        max_timestamp_micros: to_micros(stats.max_timestamp),
                        min_required_timestamp_micros: None,
                    });
            }

            // build backend_data (to send to controller in SubtaskCheckpointMetadata)
            // and new current_files
            let mut backend_data = vec![];
            let mut new_current_files: HashMap<char, BTreeMap<u32, Vec<ParquetStoreData>>> =
                HashMap::new();

            for (table, epoch_files) in self.current_files.drain() {
                let table_descriptor = self.table_descriptors.get(&table).unwrap();
                for (epoch, files) in epoch_files {
                    if table_descriptor.table_type() == TableType::Global && epoch < cp.epoch {
                        continue;
                    }
                    for file in files {
                        if table_descriptor.delete_behavior()
                            == TableDeleteBehavior::NoReadsBeforeWatermark
                        {
                            if let Some(checkpoint_watermark) = cp.watermark {
                                if file.max_timestamp_micros
                                    < to_micros(checkpoint_watermark)
                                        - table_descriptor.retention_micros
                                {
                                    // this file is not needed by the new checkpoint
                                    // because its data is older than the watermark
                                    continue;
                                }
                            }
                        }
                        backend_data.push(arroyo_rpc::grpc::BackendData {
                            backend_data: Some(BackendData::ParquetStore(file.clone())),
                        });
                        new_current_files
                            .entry(table)
                            .or_default()
                            .entry(cp.epoch)
                            .or_default()
                            .push(file);
                    }
                }
            }
            self.current_files = new_current_files;

            // send controller the subtask metadata
            let subtask_metadata = SubtaskCheckpointMetadata {
                subtask_index: self.task_info.task_index as u32,
                start_time: to_micros(cp.time),
                finish_time: to_micros(SystemTime::now()),
                has_state: !backend_data.is_empty(),
                tables: self.table_descriptors.values().cloned().collect(),
                watermark: cp.watermark.map(to_micros),
                backend_data,
                bytes: bytes as u64,
            };
            self.control_tx
                .send(ControlResp::CheckpointCompleted(CheckpointCompleted {
                    checkpoint_epoch: cp.epoch,
                    operator_id: self.task_info.operator_id.clone(),
                    subtask_metadata,
                }))
                .await
                .unwrap();
            if cp.then_stop {
                self.finish_tx.take().unwrap().send(()).unwrap();
                return Ok(false);
            }
        }
        Ok(true)
    }
}
