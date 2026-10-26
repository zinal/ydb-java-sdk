package tech.ydb.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.table.values.Value;

/**
 * One decoded changefeed record.
 * JSON and binary payloads produce the same object.
 * <p>
 * The shape follows what ydb-materializer reads from a JSON changefeed:
 * primary key in table order, erase versus upsert/reset, and either full
 * row images or the partial column map of {@code UPDATES} mode.
 * {@code step} and {@code txId} are the virtual timestamp. The topic write
 * time is not part of the record.
 */
public final class CdcRecord {
    /**
     * Kind of changefeed message.
     */
    public enum Kind {
        DATA,
        HEARTBEAT,
        SCHEMA
    }

    /**
     * Row operation. {@link #NONE} is used for heartbeats and schema records.
     * {@link #UPSERT} is the JSON {@code update} flag, {@link #RESET} is {@code reset},
     * {@link #ERASE} is {@code erase}.
     */
    public enum Operation {
        NONE,
        UPSERT,
        RESET,
        ERASE
    }

    private final Kind kind;
    private final Operation operation;
    private final List<CdcCell> key;
    @Nullable
    private final Map<String, Value<?>> updateColumns;
    @Nullable
    private final Map<String, Value<?>> resetColumns;
    @Nullable
    private final Map<String, Value<?>> oldImage;
    @Nullable
    private final Map<String, Value<?>> newImage;
    @Nullable
    private final Long step;
    @Nullable
    private final Long txId;
    @Nullable
    private final String user;
    @Nullable
    private final String traceId;
    @Nullable
    private final CdcTableSchema tableSchema;

    private CdcRecord(Builder builder) {
        this.kind = builder.kind;
        this.operation = builder.operation;
        this.key = Collections.unmodifiableList(new ArrayList<CdcCell>(builder.key));
        this.updateColumns = freeze(builder.updateColumns);
        this.resetColumns = freeze(builder.resetColumns);
        this.oldImage = freeze(builder.oldImage);
        this.newImage = freeze(builder.newImage);
        this.step = builder.step;
        this.txId = builder.txId;
        this.user = builder.user;
        this.traceId = builder.traceId;
        this.tableSchema = builder.tableSchema;
    }

    static Builder builder(Kind kind) {
        return new Builder(kind);
    }

    public Kind getKind() {
        return kind;
    }

    public Operation getOperation() {
        return operation;
    }

    public boolean isErase() {
        return operation == Operation.ERASE;
    }

    public List<CdcCell> getKey() {
        return key;
    }

    /**
     * Columns of a JSON {@code update} object, or the value cells of a binary upsert.
     * {@code null} when the record has no update section. An empty map is the
     * operation flag without column values ({@code KEYS_ONLY}, or image mode).
     */
    @Nullable
    public Map<String, Value<?>> getUpdateColumns() {
        return updateColumns;
    }

    /**
     * Columns of a JSON {@code reset} object, or the value cells of a binary replacement.
     */
    @Nullable
    public Map<String, Value<?>> getResetColumns() {
        return resetColumns;
    }

    /**
     * @return old image without primary-key columns, or {@code null} when the record has none
     */
    @Nullable
    public Map<String, Value<?>> getOldImage() {
        return oldImage;
    }

    /**
     * @return new image without primary-key columns, or {@code null} when the record has none
     */
    @Nullable
    public Map<String, Value<?>> getNewImage() {
        return newImage;
    }

    /**
     * Old image with primary-key columns filled in, or {@code null} when there is no old image.
     */
    @Nullable
    public Map<String, Value<?>> rowBefore() {
        if (oldImage == null) {
            return null;
        }
        return withKey(oldImage);
    }

    /**
     * After-image suitable for applying the change.
     * A full new image includes primary-key columns. A partial {@code UPDATES} map
     * does not: unspecified columns keep their previous values on upsert and become
     * defaults on reset. {@code null} for erase, keys-only, heartbeat and schema records.
     */
    @Nullable
    public Map<String, Value<?>> rowAfter() {
        if (newImage != null) {
            return withKey(newImage);
        }
        if (updateColumns != null && !updateColumns.isEmpty()) {
            return updateColumns;
        }
        if (resetColumns != null && !resetColumns.isEmpty()) {
            return resetColumns;
        }
        return null;
    }

    @Nullable
    public Long getStep() {
        return step;
    }

    @Nullable
    public Long getTxId() {
        return txId;
    }

    @Nullable
    public String getUser() {
        return user;
    }

    @Nullable
    public String getTraceId() {
        return traceId;
    }

    /**
     * @return schema snapshot carried by a schema record, otherwise {@code null}
     */
    @Nullable
    public CdcTableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CdcRecord record = (CdcRecord) other;
        return kind == record.kind
                && operation == record.operation
                && key.equals(record.key)
                && Objects.equals(updateColumns, record.updateColumns)
                && Objects.equals(resetColumns, record.resetColumns)
                && Objects.equals(oldImage, record.oldImage)
                && Objects.equals(newImage, record.newImage)
                && Objects.equals(step, record.step)
                && Objects.equals(txId, record.txId)
                && Objects.equals(user, record.user)
                && Objects.equals(traceId, record.traceId)
                && Objects.equals(tableSchema, record.tableSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, operation, key, updateColumns, resetColumns, oldImage, newImage,
                step, txId, user, traceId, tableSchema);
    }

    @Override
    public String toString() {
        return "CdcRecord{" + kind + " " + operation + " key=" + key + "}";
    }

    private Map<String, Value<?>> withKey(Map<String, Value<?>> image) {
        LinkedHashMap<String, Value<?>> row = new LinkedHashMap<String, Value<?>>(image);
        for (CdcCell cell : key) {
            if (!row.containsKey(cell.getName())) {
                row.put(cell.getName(), cell.getValue());
            }
        }
        return Collections.unmodifiableMap(row);
    }

    @Nullable
    private static Map<String, Value<?>> freeze(@Nullable Map<String, Value<?>> columns) {
        if (columns == null) {
            return null;
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, Value<?>>(columns));
    }

    static final class Builder {
        private final Kind kind;
        private Operation operation = Operation.NONE;
        private final List<CdcCell> key = new ArrayList<CdcCell>();
        private Map<String, Value<?>> updateColumns;
        private Map<String, Value<?>> resetColumns;
        private Map<String, Value<?>> oldImage;
        private Map<String, Value<?>> newImage;
        private Long step;
        private Long txId;
        private String user;
        private String traceId;
        private CdcTableSchema tableSchema;

        private Builder(Kind recordKind) {
            this.kind = recordKind;
        }

        Builder operation(Operation value) {
            this.operation = value;
            return this;
        }

        Builder key(List<CdcCell> cells) {
            key.addAll(cells);
            return this;
        }

        Builder updateColumns(Map<String, Value<?>> columns) {
            this.updateColumns = columns;
            return this;
        }

        Builder resetColumns(Map<String, Value<?>> columns) {
            this.resetColumns = columns;
            return this;
        }

        Builder oldImage(Map<String, Value<?>> columns) {
            this.oldImage = columns;
            return this;
        }

        Builder newImage(Map<String, Value<?>> columns) {
            this.newImage = columns;
            return this;
        }

        Builder virtualTimestamp(long virtualStep, long virtualTxId) {
            this.step = Long.valueOf(virtualStep);
            this.txId = Long.valueOf(virtualTxId);
            return this;
        }

        Builder user(String value) {
            this.user = value;
            return this;
        }

        Builder traceId(String value) {
            this.traceId = value;
            return this;
        }

        Builder tableSchema(CdcTableSchema schema) {
            this.tableSchema = schema;
            return this;
        }

        CdcRecord build() {
            return new CdcRecord(this);
        }
    }
}
