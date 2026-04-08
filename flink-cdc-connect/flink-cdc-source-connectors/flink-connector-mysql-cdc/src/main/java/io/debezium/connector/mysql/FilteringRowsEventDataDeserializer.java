/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

/**
 * A filtering wrapper around row event data deserializers (WRITE_ROWS, UPDATE_ROWS, DELETE_ROWS).
 *
 * <p>When enabled, this deserializer reads the table ID from the first 6 bytes of the event data,
 * looks up the table name via the TABLE_MAP event cache, and checks whether the table is in the
 * captured table list. If the table is NOT captured, it returns an empty EventData (with tableId
 * set but rows empty) to skip the expensive row-by-row deserialization. The remaining bytes are
 * automatically skipped by {@code EventDeserializer.deserializeEventData()} via {@code
 * skipToTheEndOfTheBlock()}.
 *
 * <p>If the table IS captured, it resets the stream position and delegates to the original
 * deserializer for full deserialization.
 */
public class FilteringRowsEventDataDeserializer<T extends EventData>
        implements EventDataDeserializer<T> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(FilteringRowsEventDataDeserializer.class);

    /** The length in bytes of the table ID field in MySQL binlog row events. */
    private static final int TABLE_ID_LENGTH = 6;

    /** The type of row event this deserializer handles. */
    public enum RowEventType {
        WRITE,
        UPDATE,
        DELETE
    }

    private final EventDataDeserializer<T> delegate;
    private final Map<Long, TableMapEventData> tableMapEventByTableId;
    private final Predicate<TableId> tableFilter;
    private final RowEventType rowEventType;

    public FilteringRowsEventDataDeserializer(
            EventDataDeserializer<T> delegate,
            Map<Long, TableMapEventData> tableMapEventByTableId,
            Predicate<TableId> tableFilter,
            RowEventType rowEventType) {
        this.delegate = delegate;
        this.tableMapEventByTableId = tableMapEventByTableId;
        this.tableFilter = tableFilter;
        this.rowEventType = rowEventType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(ByteArrayInputStream inputStream) throws IOException {
        // Read all remaining bytes from the stream so we can inspect the table ID
        // without relying on mark/reset (which may not be supported by the underlying stream).
        int available = inputStream.available();
        byte[] allBytes = inputStream.read(available);

        // The first 6 bytes encode the table number (same format as all row events)
        long tableNumber = 0;
        for (int i = 0; i < TABLE_ID_LENGTH; i++) {
            tableNumber |= ((long) (allBytes[i] & 0xFF)) << (i * 8);
        }

        // Look up the table name from the TABLE_MAP event cache
        TableMapEventData tableMapEvent = tableMapEventByTableId.get(tableNumber);
        if (tableMapEvent != null) {
            String database = tableMapEvent.getDatabase();
            String table = tableMapEvent.getTable();
            TableId tableId = new TableId(database, null, table);

            if (!tableFilter.test(tableId)) {
                // Table is NOT captured, skip deserialization by returning empty EventData
                LOGGER.debug(
                        "Skipping deserialization for non-captured table: {}.{}", database, table);
                return createEmptyEventData(tableNumber);
            }
        }

        ByteArrayInputStream newStream = new ByteArrayInputStream(allBytes);
        return delegate.deserialize(newStream);
    }

    @SuppressWarnings("unchecked")
    private T createEmptyEventData(long tableNumber) {
        switch (rowEventType) {
            case WRITE:
                WriteRowsEventData writeData = new WriteRowsEventData();
                writeData.setTableId(tableNumber);
                writeData.setRows(Collections.emptyList());
                return (T) writeData;
            case UPDATE:
                UpdateRowsEventData updateData = new UpdateRowsEventData();
                updateData.setTableId(tableNumber);
                updateData.setRows(Collections.emptyList());
                return (T) updateData;
            case DELETE:
                DeleteRowsEventData deleteData = new DeleteRowsEventData();
                deleteData.setTableId(tableNumber);
                deleteData.setRows(Collections.emptyList());
                return (T) deleteData;
            default:
                throw new IllegalStateException("Unknown row event type: " + rowEventType);
        }
    }
}
