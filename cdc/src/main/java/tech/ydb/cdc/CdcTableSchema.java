package tech.ydb.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.values.Type;

/**
 * Snapshot of a source table schema.
 * Column indexes of the binary format are positions of non-key columns sorted by UTF-8 name.
 */
public final class CdcTableSchema {
    private final String tablePath;
    private final long schemaVersion;
    @Nullable
    private final Long step;
    @Nullable
    private final Long txId;
    private final List<String> primaryKey;
    private final Map<String, CdcColumn> columns;
    private final List<CdcColumn> valueColumns;
    private final Map<String, CdcIndex> indexes;

    private CdcTableSchema(Builder builder) {
        this.tablePath = builder.tablePath;
        this.schemaVersion = builder.schemaVersion;
        this.step = builder.step;
        this.txId = builder.txId;
        this.primaryKey = Collections.unmodifiableList(new ArrayList<String>(builder.primaryKey));
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<String, CdcColumn>(builder.columns));
        this.indexes = Collections.unmodifiableMap(new LinkedHashMap<String, CdcIndex>(builder.indexes));
        this.valueColumns = Collections.unmodifiableList(valueColumns(builder));
    }

    public static Builder builder(String tablePath) {
        return new Builder(tablePath);
    }

    public static CdcTableSchema fromTableDescription(String tablePath, TableDescription description) {
        Builder builder = builder(tablePath);
        for (TableColumn column : description.getColumns()) {
            builder.column(column.getName(), column.getType(), column.getFamily());
        }
        for (String key : description.getPrimaryKeys()) {
            builder.key(key);
        }
        for (TableIndex index : description.getIndexes()) {
            builder.index(index.getName(), indexTypeName(index.getType()), index.getColumns(), index.getDataColumns());
        }
        return builder.build();
    }

    /**
     * Accepts a CDC schema-change document ({@code tableChanges}) or the canonical snapshot
     * stored in the service table.
     */
    public static CdcTableSchema fromChangeDocument(String tablePath, String json) {
        JsonElement root = JsonParser.parseString(json);
        if (!root.isJsonObject()) {
            throw new CdcParseException("CDC schema document is not an object", 0);
        }
        return fromChangeDocument(tablePath, root.getAsJsonObject());
    }

    public static CdcTableSchema fromChangeDocument(String tablePath, JsonObject root) {
        JsonObject table = root;
        Long step = null;
        Long txId = null;
        if (root.has("tableChanges")) {
            JsonArray changes = root.getAsJsonArray("tableChanges");
            if (changes.size() == 0) {
                throw new CdcParseException("CDC schema document has no tableChanges", 0);
            }
            JsonObject change = changes.get(changes.size() - 1).getAsJsonObject();
            if (!change.has("table") || !change.get("table").isJsonObject()) {
                throw new CdcParseException("CDC schema change has no table", 0);
            }
            table = change.getAsJsonObject("table");
        }
        if (root.has("ts") && root.get("ts").isJsonArray()) {
            JsonArray ts = root.getAsJsonArray("ts");
            if (ts.size() >= 2) {
                step = Long.valueOf(CdcValues.uint64(ts.get(0)));
                txId = Long.valueOf(CdcValues.uint64(ts.get(1)));
            }
        }
        if (!table.has("columns") || !table.get("columns").isJsonObject()) {
            throw new CdcParseException("CDC schema has no columns", 0);
        }

        Builder builder = builder(tablePath);
        if (step != null) {
            builder.virtualTimestamp(step.longValue(), txId.longValue());
        }
        if (table.has("schemaVersion") && !table.get("schemaVersion").isJsonNull()) {
            builder.schemaVersion(CdcValues.uint64(table.get("schemaVersion")));
        }
        JsonObject columns = table.getAsJsonObject("columns");
        for (Map.Entry<String, JsonElement> entry : columns.entrySet()) {
            JsonObject spec = entry.getValue().getAsJsonObject();
            String typeName = spec.get("type").getAsString();
            String family = spec.has("family") && !spec.get("family").isJsonNull()
                    ? spec.get("family").getAsString() : null;
            builder.column(entry.getKey(), CdcTypes.parse(typeName), typeName, family);
        }
        if (table.has("primaryKeyColumnNames")) {
            for (JsonElement key : table.getAsJsonArray("primaryKeyColumnNames")) {
                builder.key(key.getAsString());
            }
        }
        if (table.has("indexes") && table.get("indexes").isJsonObject()) {
            JsonObject indexes = table.getAsJsonObject("indexes");
            for (Map.Entry<String, JsonElement> entry : indexes.entrySet()) {
                JsonObject spec = entry.getValue().getAsJsonObject();
                builder.index(entry.getKey(),
                        spec.has("type") ? spec.get("type").getAsString() : "",
                        stringList(spec.get("indexColumns")),
                        stringList(spec.get("dataColumns")));
            }
        }
        return builder.build();
    }

    public String toJson() {
        JsonObject root = new JsonObject();
        root.addProperty("schemaVersion", schemaVersion);
        JsonArray keys = new JsonArray();
        for (String key : primaryKey) {
            keys.add(key);
        }
        root.add("primaryKeyColumnNames", keys);
        JsonObject columnJson = new JsonObject();
        for (CdcColumn column : columns.values()) {
            JsonObject spec = new JsonObject();
            spec.addProperty("type", column.getTypeName());
            if (column.getFamily() != null) {
                spec.addProperty("family", column.getFamily());
            }
            columnJson.add(column.getName(), spec);
        }
        root.add("columns", columnJson);
        JsonObject indexJson = new JsonObject();
        for (CdcIndex index : indexes.values()) {
            JsonObject spec = new JsonObject();
            spec.addProperty("type", index.getTypeName());
            spec.add("indexColumns", jsonStrings(index.getColumns()));
            spec.add("dataColumns", jsonStrings(index.getDataColumns()));
            indexJson.add(index.getName(), spec);
        }
        root.add("indexes", indexJson);
        if (step != null && txId != null) {
            JsonArray ts = new JsonArray();
            ts.add(step.longValue());
            ts.add(txId.longValue());
            root.add("ts", ts);
        }
        return root.toString();
    }

    public String getTablePath() {
        return tablePath;
    }

    public long getSchemaVersion() {
        return schemaVersion;
    }

    @Nullable
    public Long getStep() {
        return step;
    }

    @Nullable
    public Long getTxId() {
        return txId;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public Map<String, CdcColumn> getColumns() {
        return columns;
    }

    /**
     * Non-key columns in the order used as binary column indexes.
     */
    public List<CdcColumn> getValueColumns() {
        return valueColumns;
    }

    public Map<String, CdcIndex> getIndexes() {
        return indexes;
    }

    public boolean isWideIndex() {
        return valueColumns.size() > 127;
    }

    @Nullable
    public CdcColumn column(String name) {
        return columns.get(name);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CdcTableSchema schema = (CdcTableSchema) other;
        return schemaVersion == schema.schemaVersion
                && tablePath.equals(schema.tablePath)
                && Objects.equals(step, schema.step)
                && Objects.equals(txId, schema.txId)
                && primaryKey.equals(schema.primaryKey)
                && columns.equals(schema.columns)
                && indexes.equals(schema.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, schemaVersion, step, txId, primaryKey, columns, indexes);
    }

    private static List<CdcColumn> valueColumns(Builder builder) {
        List<CdcColumn> values = new ArrayList<CdcColumn>();
        for (CdcColumn column : builder.columns.values()) {
            if (!builder.primaryKey.contains(column.getName())) {
                values.add(column);
            }
        }
        Collections.sort(values, new java.util.Comparator<CdcColumn>() {
            @Override
            public int compare(CdcColumn left, CdcColumn right) {
                return CdcNames.UTF8.compare(left.getName(), right.getName());
            }
        });
        return values;
    }

    private static List<String> stringList(@Nullable JsonElement element) {
        List<String> values = new ArrayList<String>();
        if (element != null && element.isJsonArray()) {
            for (JsonElement item : element.getAsJsonArray()) {
                values.add(item.getAsString());
            }
        }
        return values;
    }

    private static JsonArray jsonStrings(List<String> values) {
        JsonArray array = new JsonArray();
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    private static String indexTypeName(TableIndex.Type type) {
        switch (type) {
            case GLOBAL:
                return "GlobalSync";
            case GLOBAL_ASYNC:
                return "GlobalAsync";
            case GLOBAL_UNIQUE:
                return "GlobalUnique";
            case GLOBAL_JSON:
                return "GlobalJson";
            default:
                return type.name();
        }
    }

    /**
     * Builds an immutable {@link CdcTableSchema}.
     */
    public static final class Builder {
        private final String tablePath;
        private long schemaVersion;
        private Long step;
        private Long txId;
        private final List<String> primaryKey = new ArrayList<String>();
        private final Map<String, CdcColumn> columns = new LinkedHashMap<String, CdcColumn>();
        private final Map<String, CdcIndex> indexes = new LinkedHashMap<String, CdcIndex>();

        private Builder(String tablePath) {
            this.tablePath = Objects.requireNonNull(tablePath, "tablePath");
        }

        public Builder schemaVersion(long version) {
            this.schemaVersion = version;
            return this;
        }

        public Builder virtualTimestamp(long virtualStep, long virtualTxId) {
            this.step = Long.valueOf(virtualStep);
            this.txId = Long.valueOf(virtualTxId);
            return this;
        }

        public Builder column(String name, Type type) {
            return column(name, type, CdcTypes.format(type), null);
        }

        public Builder column(String name, Type type, @Nullable String family) {
            return column(name, type, CdcTypes.format(type), family);
        }

        public Builder column(String name, @Nullable Type type, String typeName, @Nullable String family) {
            columns.put(name, new CdcColumn(name, type, typeName, family));
            return this;
        }

        public Builder key(String name) {
            if (!columns.containsKey(name)) {
                throw new IllegalArgumentException("primary key column is not in the schema: " + name);
            }
            if (!primaryKey.contains(name)) {
                primaryKey.add(name);
            }
            return this;
        }

        public Builder key(String name, Type type) {
            column(name, type);
            return key(name);
        }

        public Builder index(String name, String typeName, List<String> keyColumns, List<String> dataColumns) {
            indexes.put(name, new CdcIndex(name, typeName, keyColumns, dataColumns));
            return this;
        }

        public CdcTableSchema build() {
            for (String key : primaryKey) {
                if (!columns.containsKey(key)) {
                    throw new IllegalArgumentException("primary key column is not in the schema: " + key);
                }
            }
            return new CdcTableSchema(this);
        }
    }
}
