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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.metrics.MySqlSourceReaderMetrics;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.Collector;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the concurrent deserialization feature of {@link MySqlRecordEmitter}.
 *
 * <p>Tests verify that when {@code deserializeParallelism > 1} and the split is a binlog split,
 * DataChangeRecord deserialization is parallelized while maintaining correct output ordering and
 * proper synchronization barrier behavior for control events.
 */
class MySqlRecordEmitterConcurrentTest {

    private static final String HEARTBEAT_SCHEMA_NAME = "io.debezium.connector.common.Heartbeat";

    private MySqlRecordEmitter<?> emitterToClose;

    @AfterEach
    void tearDown() throws Exception {
        if (emitterToClose != null) {
            emitterToClose.close();
            emitterToClose = null;
        }
    }

    @Test
    void testConcurrentDeserializationMaintainsOrder() throws Exception {
        // Verify that concurrent deserialization outputs records in the original binlog order
        int numRecords = 50;
        List<String> deserializedOrder = Collections.synchronizedList(new ArrayList<>());

        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        4,
                        (record, out) -> {
                            // Simulate some deserialization work
                            String topic = record.topic();
                            try {
                                Thread.sleep(1); // Small delay to encourage thread interleaving
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            deserializedOrder.add(topic);
                            out.collect("record-" + topic);
                        });
        emitterToClose = emitter;

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        // Create a batch of DataChangeRecords
        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add(createDataChangeRecord("topic-" + String.format("%03d", i), i * 100L));
        }

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        // Verify output order matches original order
        List<String> emittedRecords = new ArrayList<>();
        output.getEmittedRecords().forEach(emittedRecords::add);

        Assertions.assertThat(emittedRecords).hasSize(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Assertions.assertThat(emittedRecords.get(i))
                    .isEqualTo("record-topic-" + String.format("%03d", i));
        }
    }

    @Test
    void testSyncBarrierFlushesBeforeControlEvent() throws Exception {
        // Verify that a heartbeat event (sync barrier) flushes all pending DataChangeRecords
        // before being processed
        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        4,
                        (record, out) -> {
                            out.collect("data-" + record.topic());
                        });
        emitterToClose = emitter;

        // Create: 3 DataChangeRecords, then a HeartbeatEvent, then 2 more DataChangeRecords
        List<SourceRecord> records = new ArrayList<>();
        records.add(createDataChangeRecord("dcr-0", 100L));
        records.add(createDataChangeRecord("dcr-1", 200L));
        records.add(createDataChangeRecord("dcr-2", 300L));
        records.add(createHeartbeatRecord(400L));
        records.add(createDataChangeRecord("dcr-3", 500L));
        records.add(createDataChangeRecord("dcr-4", 600L));

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        List<String> emittedRecords = new ArrayList<>();
        output.getEmittedRecords().forEach(emittedRecords::add);

        // All 5 data records should be emitted (heartbeat is not emitted when
        // includeHeartbeatEvents=false)
        Assertions.assertThat(emittedRecords).hasSize(5);
        // Verify order: dcr-0, dcr-1, dcr-2 must come before dcr-3, dcr-4
        Assertions.assertThat(emittedRecords.get(0)).isEqualTo("data-dcr-0");
        Assertions.assertThat(emittedRecords.get(1)).isEqualTo("data-dcr-1");
        Assertions.assertThat(emittedRecords.get(2)).isEqualTo("data-dcr-2");
        Assertions.assertThat(emittedRecords.get(3)).isEqualTo("data-dcr-3");
        Assertions.assertThat(emittedRecords.get(4)).isEqualTo("data-dcr-4");
    }

    @Test
    void testParallelismOneFallsBackToSerial() throws Exception {
        // Verify that parallelism=1 uses the original serial path (no worker pool)
        List<String> deserializeThreadNames = Collections.synchronizedList(new ArrayList<>());

        MySqlRecordEmitter<String> emitter =
                new MySqlRecordEmitter<>(
                        new TestDeserializationSchema(
                                (record, out) -> {
                                    deserializeThreadNames.add(Thread.currentThread().getName());
                                    out.collect("serial-" + record.topic());
                                }),
                        new MySqlSourceReaderMetrics(
                                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                        false,
                        false,
                        false,
                        1); // parallelism=1, serial mode
        emitterToClose = emitter;

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        List<SourceRecord> records = new ArrayList<>();
        records.add(createDataChangeRecord("topic-0", 100L));
        records.add(createDataChangeRecord("topic-1", 200L));

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        List<String> emittedRecords = new ArrayList<>();
        output.getEmittedRecords().forEach(emittedRecords::add);
        Assertions.assertThat(emittedRecords).containsExactly("serial-topic-0", "serial-topic-1");

        // In serial mode, deserialization should happen on the calling thread (not worker threads)
        String callingThread = Thread.currentThread().getName();
        for (String threadName : deserializeThreadNames) {
            Assertions.assertThat(threadName).isEqualTo(callingThread);
        }
    }

    @Test
    void testWorkerThreadExceptionPropagation() throws Exception {
        // Verify that exceptions in worker threads are propagated to the main thread
        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        2,
                        (record, out) -> {
                            throw new RuntimeException("Simulated deserialization failure");
                        });
        emitterToClose = emitter;

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        List<SourceRecord> records = new ArrayList<>();
        records.add(createDataChangeRecord("topic-0", 100L));

        Assertions.assertThatThrownBy(
                        () -> emitter.emitRecord(new SourceRecords(records), output, splitState))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Simulated deserialization failure");
    }

    @Test
    void testCloseGracefullyShutdownsWorkerPool() throws Exception {
        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(4, (record, out) -> out.collect("test"));

        // Close should not throw
        Assertions.assertThatCode(emitter::close).doesNotThrowAnyException();
    }

    @Test
    void testOffsetUpdatedBeforeAsyncSubmission() throws Exception {
        // Verify that offset is updated on the main thread before async task is submitted
        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        2,
                        (record, out) -> {
                            out.collect("data");
                        });
        emitterToClose = emitter;

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        List<SourceRecord> records = new ArrayList<>();
        records.add(createDataChangeRecord("topic-0", 100L));
        records.add(createDataChangeRecord("topic-1", 200L));
        records.add(createDataChangeRecord("topic-2", 300L));

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        // After emitRecord returns, offset should be at the last record's position
        BinlogOffset finalOffset = splitState.getStartingOffset();
        Assertions.assertThat(finalOffset).isNotNull();
        // The offset should reflect the last DataChangeRecord processed
        Assertions.assertThat(finalOffset.getPosition()).isEqualTo(300L);
    }

    @Test
    void testUnknownElementFlushesAndSkips() throws Exception {
        // Verify that unknown elements trigger a flush and are skipped
        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        2,
                        (record, out) -> {
                            out.collect("data-" + record.topic());
                        });
        emitterToClose = emitter;

        List<SourceRecord> records = new ArrayList<>();
        records.add(createDataChangeRecord("dcr-0", 100L));
        // Create an unknown element (no special schema markers)
        records.add(createUnknownRecord(200L));
        records.add(createDataChangeRecord("dcr-1", 300L));

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        List<String> emittedRecords = new ArrayList<>();
        output.getEmittedRecords().forEach(emittedRecords::add);

        // Only data change records should be emitted, unknown is skipped
        Assertions.assertThat(emittedRecords).hasSize(2);
        Assertions.assertThat(emittedRecords.get(0)).isEqualTo("data-dcr-0");
        Assertions.assertThat(emittedRecords.get(1)).isEqualTo("data-dcr-1");
    }

    @Test
    void testConcurrentDeserializationUsesWorkerThreads() throws Exception {
        // Verify that concurrent mode actually uses worker threads (not the main thread)
        List<String> deserializeThreadNames = Collections.synchronizedList(new ArrayList<>());

        MySqlRecordEmitter<String> emitter =
                createConcurrentEmitter(
                        4,
                        (record, out) -> {
                            deserializeThreadNames.add(Thread.currentThread().getName());
                            out.collect("data");
                        });
        emitterToClose = emitter;

        MySqlBinlogSplitState splitState = createBinlogSplitState();
        TestingReaderOutput<String> output = new TestingReaderOutput<>();

        List<SourceRecord> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(createDataChangeRecord("topic-" + i, i * 100L));
        }

        emitter.emitRecord(new SourceRecords(records), output, splitState);

        // All deserialization should happen on worker threads, not the calling thread
        String callingThread = Thread.currentThread().getName();
        for (String threadName : deserializeThreadNames) {
            Assertions.assertThat(threadName)
                    .startsWith("mysql-cdc-deserialize-worker")
                    .isNotEqualTo(callingThread);
        }
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    private MySqlRecordEmitter<String> createConcurrentEmitter(
            int parallelism, DeserializeAction action) {
        return new MySqlRecordEmitter<>(
                new TestDeserializationSchema(action),
                new MySqlSourceReaderMetrics(
                        UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()),
                false,
                false,
                false,
                parallelism);
    }

    private MySqlBinlogSplitState createBinlogSplitState() {
        return new MySqlBinlogSplitState(
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofEarliest(),
                        BinlogOffset.ofNonStopping(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        0));
    }

    /**
     * Create a DataChangeRecord that passes RecordUtils.isDataChangeRecord() check. The record must
     * have a value schema with an "op" field and a non-null operation string. It also needs a
     * "source" struct with "ts_ms" for getMessageTimestamp() and a top-level "ts_ms" for
     * getFetchTimestamp().
     */
    private SourceRecord createDataChangeRecord(String topic, long position) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .field("file", Schema.STRING_SCHEMA)
                        .field("pos", Schema.INT64_SCHEMA)
                        .build();

        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                        .build();

        Struct source =
                new Struct(sourceSchema)
                        .put(Envelope.FieldName.TIMESTAMP, System.currentTimeMillis())
                        .put("db", "test_db")
                        .put("table", "test_table")
                        .put("file", "mysql-bin.000001")
                        .put("pos", position);

        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.OPERATION, Envelope.Operation.CREATE.code())
                        .put(Envelope.FieldName.SOURCE, source)
                        .put(Envelope.FieldName.TIMESTAMP, System.currentTimeMillis());

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql_binlog_source"),
                offset,
                topic,
                null,
                null,
                valueSchema,
                value);
    }

    /**
     * Create a HeartbeatRecord that passes RecordUtils.isHeartbeatEvent() check. The record must
     * have a value schema with name "io.debezium.connector.common.Heartbeat".
     */
    private SourceRecord createHeartbeatRecord(long position) {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name(HEARTBEAT_SCHEMA_NAME)
                        .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                        .build();

        Struct value =
                new Struct(valueSchema)
                        .put(Envelope.FieldName.TIMESTAMP, System.currentTimeMillis());

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql_binlog_source"),
                offset,
                "__debezium-heartbeat.mysql_binlog_source",
                null,
                null,
                valueSchema,
                value);
    }

    /**
     * Create an unknown record that does not match any known event type. It must not have an "op"
     * field (not DataChangeRecord), not have heartbeat schema name, not have watermark signal
     * schema name, and not have schema change key schema name.
     */
    private SourceRecord createUnknownRecord(long position) {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("some.unknown.schema")
                        .field("unknown_field", Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema).put("unknown_field", "data");

        Map<String, Object> offset = new HashMap<>();
        offset.put("file", "mysql-bin.000001");
        offset.put("pos", position);

        return new SourceRecord(
                Collections.singletonMap("server", "mysql_binlog_source"),
                offset,
                "some.unknown.topic",
                null,
                null,
                valueSchema,
                value);
    }

    /** Functional interface for test deserialization logic. */
    @FunctionalInterface
    interface DeserializeAction {
        void accept(SourceRecord record, Collector<String> out) throws Exception;
    }

    /** Test implementation of DebeziumDeserializationSchema. */
    private static class TestDeserializationSchema
            implements DebeziumDeserializationSchema<String> {

        private final DeserializeAction action;

        TestDeserializationSchema(DeserializeAction action) {
            this.action = action;
        }

        @Override
        public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
            action.accept(record, out);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
