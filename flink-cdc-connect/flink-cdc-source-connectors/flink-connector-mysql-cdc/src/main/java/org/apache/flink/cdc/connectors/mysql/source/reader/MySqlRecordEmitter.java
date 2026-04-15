/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;

import io.debezium.document.Array;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The {@link RecordEmitter} implementation for {@link MySqlSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the binlog reader to
 * emit records rather than emit the records directly.
 *
 * <p>When {@code deserializeParallelism > 1} and the split is a binlog split, DataChangeRecord
 * deserialization tasks are submitted to a worker thread pool for concurrent execution. Control
 * events (Watermark, SchemaChange, Heartbeat, TransactionMetadata) act as synchronization barriers
 * that flush all pending async tasks before being processed on the main thread.
 */
public class MySqlRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, MySqlSplitState>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordEmitter.class);
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
            new FlinkJsonTableChangeSerializer();

    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    private final MySqlSourceReaderMetrics sourceReaderMetrics;
    private final boolean includeSchemaChanges;
    private final boolean includeHeartbeatEvents;
    private final boolean includeTransactionMetadataEvents;
    private final OutputCollector<T> outputCollector;

    /** Worker thread pool for concurrent deserialization. Null when parallelism is 1. */
    private final ExecutorService workerPool;

    /** Ordered list of pending async deserialization tasks. */
    private List<Future<DeserializeResult<T>>> pendingFutures;

    /** Index tracking how far we have drained completed futures in pipeline mode. */
    private int drainIndex;

    public MySqlRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            MySqlSourceReaderMetrics sourceReaderMetrics,
            boolean includeSchemaChanges,
            boolean includeHeartbeatEvents,
            boolean includeTransactionMetadataEvents,
            int deserializeParallelism) {
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.sourceReaderMetrics = sourceReaderMetrics;
        this.includeSchemaChanges = includeSchemaChanges;
        this.includeHeartbeatEvents = includeHeartbeatEvents;
        this.includeTransactionMetadataEvents = includeTransactionMetadataEvents;
        this.outputCollector = new OutputCollector<>();

        if (deserializeParallelism > 1) {
            this.workerPool =
                    new ThreadPoolExecutor(
                            deserializeParallelism,
                            deserializeParallelism,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(),
                            r -> {
                                Thread t = new Thread(r, "mysql-cdc-deserialize-worker");
                                t.setDaemon(true);
                                return t;
                            });
            LOG.info(
                    "Created deserialization worker pool with {} threads.", deserializeParallelism);
        } else {
            this.workerPool = null;
        }
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        // Determine whether to use concurrent mode
        boolean concurrentMode = workerPool != null && splitState.isBinlogSplitState();

        if (concurrentMode) {
            pendingFutures = new ArrayList<>();
            drainIndex = 0;
            try {
                final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
                while (elementIterator.hasNext()) {
                    processElement(elementIterator.next(), output, splitState);
                }
                flush(pendingFutures, output);
            } finally {
                pendingFutures = null;
                drainIndex = 0;
            }
        } else {
            final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
            while (elementIterator.hasNext()) {
                processElement(elementIterator.next(), output, splitState);
            }
        }
    }

    protected void processElement(
            SourceRecord element, SourceOutput<T> output, MySqlSplitState splitState)
            throws Exception {
        if (RecordUtils.isWatermarkEvent(element)) {
            Preconditions.checkState(pendingFutures == null, "pendingFutures should be null");
            BinlogOffset watermark = RecordUtils.getWatermark(element);
            if (RecordUtils.isHighWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (RecordUtils.isSchemaChangeEvent(element) && splitState.isBinlogSplitState()) {
            // Sync barrier: flush pending tasks before processing
            if (pendingFutures != null) {
                flush(pendingFutures, output);
            }
            HistoryRecord historyRecord = RecordUtils.getHistoryRecord(element);
            Array tableChanges =
                    historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
            TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
            for (TableChanges.TableChange tableChange : changes) {
                splitState.asBinlogSplitState().recordSchema(tableChange.getId(), tableChange);
            }
            if (includeSchemaChanges) {
                BinlogOffset position = RecordUtils.getBinlogPosition(element);
                splitState.asBinlogSplitState().setStartingOffset(position);
                emitElement(element, output);
            }
        } else if (RecordUtils.isDataChangeRecord(element)) {
            updateStartingOffsetForSplit(splitState, element);
            reportMetrics(element);
            if (pendingFutures != null) {
                pendingFutures.add(submitDeserializeTask(element));
                drainCompleted(pendingFutures, output);
            } else {
                emitElement(element, output);
            }
        } else if (RecordUtils.isHeartbeatEvent(element)) {
            // Sync barrier: flush pending tasks before processing
            if (pendingFutures != null) {
                flush(pendingFutures, output);
            }
            updateStartingOffsetForSplit(splitState, element);
            if (includeHeartbeatEvents) {
                emitElement(element, output);
            }
        } else if (RecordUtils.isTransactionMetadataEvent(element)) {
            // Sync barrier: flush pending tasks before processing
            if (pendingFutures != null) {
                flush(pendingFutures, output);
            }
            updateStartingOffsetForSplit(splitState, element);
            if (includeTransactionMetadataEvents) {
                emitElement(element, output);
            }
        } else {
            if (pendingFutures != null) {
                flush(pendingFutures, output);
            }
            LOG.info("Meet unknown element {}, just skip.", element);
        }
    }

    /**
     * Submit a deserialization task for a DataChangeRecord to the worker pool. The task uses a
     * thread-local BufferingCollector to collect deserialized records, avoiding contention on the
     * shared SourceOutput.
     */
    private Future<DeserializeResult<T>> submitDeserializeTask(SourceRecord element) {
        final Long messageTimestamp = RecordUtils.getMessageTimestamp(element);
        return workerPool.submit(
                () -> {
                    BufferingCollector<T> collector = new BufferingCollector<>(messageTimestamp);
                    debeziumDeserializationSchema.deserialize(element, collector);
                    return new DeserializeResult<>(
                            collector.collectedRecords, collector.messageTimestamp);
                });
    }

    /**
     * Eagerly drain completed futures from the head of the pending list without blocking. This
     * enables pipelining: records are output as soon as their deserialization finishes, while new
     * tasks continue to be submitted. Uses {@link #drainIndex} to avoid O(n) list removal.
     */
    private void drainCompleted(List<Future<DeserializeResult<T>>> futures, SourceOutput<T> output)
            throws Exception {
        while (drainIndex < futures.size() && futures.get(drainIndex).isDone()) {
            outputDeserializeResult(futures.get(drainIndex), output);
            drainIndex++;
        }
    }

    /**
     * Flush all remaining pending async deserialization tasks. Starts from {@link #drainIndex}
     * (skipping already-drained futures), blocks on each to get the result, and outputs records on
     * the main thread. Clears the list and resets drainIndex after completion.
     */
    private void flush(List<Future<DeserializeResult<T>>> futures, SourceOutput<T> output)
            throws Exception {
        Preconditions.checkArgument(futures != null);
        for (int i = drainIndex; i < futures.size(); i++) {
            outputDeserializeResult(futures.get(i), output);
        }
        futures.clear();
        drainIndex = 0;
    }

    /** Extract the result from a future and output all records to the SourceOutput. */
    private void outputDeserializeResult(
            Future<DeserializeResult<T>> future, SourceOutput<T> output) throws Exception {
        DeserializeResult<T> result;
        try {
            result = future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
        Long timestamp = result.messageTimestamp;
        for (T record : result.records) {
            if (timestamp != null && timestamp > 0) {
                output.collect(record, timestamp);
            } else {
                output.collect(record);
            }
        }
    }

    private void updateStartingOffsetForSplit(MySqlSplitState splitState, SourceRecord element) {
        if (splitState.isBinlogSplitState()) {
            BinlogOffset position = RecordUtils.getBinlogPosition(element);
            splitState.asBinlogSplitState().setStartingOffset(position);
        }
    }

    private void emitElement(SourceRecord element, SourceOutput<T> output) throws Exception {
        outputCollector.output = output;
        outputCollector.currentMessageTimestamp = RecordUtils.getMessageTimestamp(element);
        debeziumDeserializationSchema.deserialize(element, outputCollector);
    }

    public void applySplit(MySqlSplit split) {}

    private void reportMetrics(SourceRecord element) {

        Long messageTimestamp = RecordUtils.getMessageTimestamp(element);

        if (messageTimestamp != null && messageTimestamp > 0L) {
            // report fetch delay
            Long fetchTimestamp = RecordUtils.getFetchTimestamp(element);
            if (fetchTimestamp != null && fetchTimestamp >= messageTimestamp) {
                // report fetch delay
                sourceReaderMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (workerPool != null) {
            LOG.info("Shutting down deserialization worker pool...");
            workerPool.shutdown();
            try {
                if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOG.warn("Worker pool did not terminate within 30 seconds, forcing shutdown.");
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for worker pool shutdown.", e);
                workerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("Deserialization worker pool shut down.");
        }
    }

    // -------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------

    /**
     * Collector used by worker threads during concurrent deserialization. Buffers records locally
     * instead of writing to SourceOutput directly, ensuring thread safety.
     */
    private static class BufferingCollector<T> implements Collector<T> {
        final List<T> collectedRecords;
        final Long messageTimestamp;

        BufferingCollector(Long messageTimestamp) {
            this.collectedRecords = new ArrayList<>();
            this.messageTimestamp = messageTimestamp;
        }

        @Override
        public void collect(T record) {
            collectedRecords.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    /** Result of an async deserialization task. */
    private static class DeserializeResult<T> {
        final List<T> records;
        final Long messageTimestamp;

        DeserializeResult(List<T> records, Long messageTimestamp) {
            this.records = records;
            this.messageTimestamp = messageTimestamp;
        }
    }

    /**
     * Collector used for synchronous (serial mode) deserialization. Writes directly to
     * SourceOutput.
     */
    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;
        private Long currentMessageTimestamp;

        @Override
        public void collect(T record) {
            if (currentMessageTimestamp != null && currentMessageTimestamp > 0) {
                // Only binlog event contains a valid timestamp. We use the output with timestamp to
                // report the event time and let the source operator to report
                // "currentEmitEventTimeLag" correctly.
                output.collect(record, currentMessageTimestamp);
            } else {
                // Records in snapshot mode have a zero timestamp in the message. We use the output
                // without timestamp to collect the record. Metric "currentEmitEventTimeLag" will
                // not be updated in the source operator in this case.
                output.collect(record);
            }
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
