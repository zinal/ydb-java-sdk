package tech.ydb.cdc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import tech.ydb.table.values.Value;

/**
 * Decodes one YDB JSON changefeed record.
 */
final class JsonCdcDecoder {
    private JsonCdcDecoder() {
    }

    static CdcRecord decode(JsonObject root, CdcTableSchema schema, String tablePath) {
        if (root.has("tableChanges")) {
            return schema(root, tablePath);
        }
        if (root.has("resolved")) {
            return heartbeat(root);
        }
        if (!root.has("key") || !root.get("key").isJsonArray()) {
            throw new CdcParseException("CDC JSON record has no key", 0);
        }
        return data(root, schema);
    }

    private static CdcRecord schema(JsonObject root, String tablePath) {
        CdcTableSchema parsed = CdcTableSchema.fromChangeDocument(tablePath, root);
        CdcRecord.Builder builder = CdcRecord.builder(CdcRecord.Kind.SCHEMA).tableSchema(parsed);
        if (parsed.getStep() != null && parsed.getTxId() != null) {
            builder.virtualTimestamp(parsed.getStep().longValue(), parsed.getTxId().longValue());
        }
        return builder.build();
    }

    private static CdcRecord heartbeat(JsonObject root) {
        JsonArray resolved = root.getAsJsonArray("resolved");
        if (resolved.size() < 2) {
            throw new CdcParseException("CDC barrier has no virtual timestamp", 0);
        }
        return CdcRecord.builder(CdcRecord.Kind.HEARTBEAT)
                .virtualTimestamp(CdcValues.uint64(resolved.get(0)), CdcValues.uint64(resolved.get(1)))
                .build();
    }

    private static CdcRecord data(JsonObject root, CdcTableSchema schema) {
        if (schema == null) {
            throw new CdcParseException("CDC table schema is required to decode a data record", 0);
        }
        CdcRecord.Operation operation = operation(root);
        boolean images = root.has("newImage") || root.has("oldImage");
        CdcRecord.Builder builder = CdcRecord.builder(CdcRecord.Kind.DATA)
                .operation(operation)
                .key(key(root.getAsJsonArray("key"), schema));
        if (root.has("oldImage")) {
            builder.oldImage(columns(root.get("oldImage"), schema));
        }
        if (root.has("newImage")) {
            builder.newImage(columns(root.get("newImage"), schema));
        }
        if (root.has("update") && !images) {
            builder.updateColumns(columns(root.get("update"), schema));
        } else if (root.has("update")) {
            builder.updateColumns(new LinkedHashMap<String, Value<?>>());
        }
        if (root.has("reset") && !images) {
            builder.resetColumns(columns(root.get("reset"), schema));
        } else if (root.has("reset")) {
            builder.resetColumns(new LinkedHashMap<String, Value<?>>());
        }
        if (root.has("ts")) {
            JsonArray ts = root.getAsJsonArray("ts");
            builder.virtualTimestamp(CdcValues.uint64(ts.get(0)), CdcValues.uint64(ts.get(1)));
        }
        if (root.has("user") && !root.get("user").isJsonNull()) {
            builder.user(root.get("user").getAsString());
        }
        if (root.has("traceId") && !root.get("traceId").isJsonNull()) {
            builder.traceId(root.get("traceId").getAsString());
        }
        return builder.build();
    }

    private static CdcRecord.Operation operation(JsonObject root) {
        int flags = 0;
        if (root.has("update")) {
            flags++;
        }
        if (root.has("reset")) {
            flags++;
        }
        if (root.has("erase")) {
            flags++;
        }
        if (flags != 1) {
            throw new CdcParseException("CDC JSON record must contain one of update, reset, erase", 0);
        }
        if (root.has("erase")) {
            return CdcRecord.Operation.ERASE;
        }
        if (root.has("reset")) {
            return CdcRecord.Operation.RESET;
        }
        return CdcRecord.Operation.UPSERT;
    }

    private static List<CdcCell> key(JsonArray values, CdcTableSchema schema) {
        List<String> names = schema.getPrimaryKey();
        if (values.size() != names.size()) {
            throw new CdcParseException("CDC key column count mismatch: record " + values.size()
                    + ", schema " + names.size(), 0);
        }
        List<CdcCell> cells = new ArrayList<CdcCell>(names.size());
        for (int i = 0; i < names.size(); i++) {
            CdcColumn column = schema.column(names.get(i));
            cells.add(new CdcCell(column.getName(), CdcValues.fromJson(values.get(i), column.getType(), 0)));
        }
        return cells;
    }

    private static Map<String, Value<?>> columns(JsonElement element, CdcTableSchema schema) {
        if (element == null || element.isJsonNull() || !element.isJsonObject()) {
            throw new CdcParseException("CDC JSON column section is not an object", 0);
        }
        LinkedHashMap<String, Value<?>> columns = new LinkedHashMap<String, Value<?>>();
        for (Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
            CdcColumn column = schema.column(entry.getKey());
            if (column == null || schema.getPrimaryKey().contains(entry.getKey())) {
                continue;
            }
            columns.put(entry.getKey(), CdcValues.fromJson(entry.getValue(), column.getType(), 0));
        }
        return columns;
    }
}
