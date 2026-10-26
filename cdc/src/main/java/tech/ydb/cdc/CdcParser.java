package tech.ydb.cdc;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;

/**
 * Parses blocks of CDC records from JSON or from the binary format.
 * <p>
 * A JSON block is one object, an array of objects, or newline-delimited objects.
 * A binary block is one record or several records concatenated. Each message of
 * {@link #parseMessages(Iterable)} is detected independently. The decoded
 * {@link CdcRecord} is the same for both formats.
 * <p>
 * The parser is bound to one source table. A schema record replaces the schema
 * used for the rest of the block and, when a {@link TableSchemaSource} is set,
 * is written there. The instance keeps that schema for later calls and is not
 * safe for concurrent use.
 */
public final class CdcParser {
    private final String tablePath;
    @Nullable
    private final TableSchemaSource schemas;
    @Nullable
    private CdcTableSchema schema;
    private final BinaryCdcDecoder.SchemaState state = new State();

    private CdcParser(String tablePath, @Nullable TableSchemaSource schemas, @Nullable CdcTableSchema schema) {
        this.tablePath = tablePath;
        this.schemas = schemas;
        this.schema = schema;
    }

    /**
     * Parser that decodes with a fixed schema. Schema records still replace it
     * for the following records, but nothing is stored externally.
     */
    public static CdcParser of(CdcTableSchema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("schema is required");
        }
        return new CdcParser(schema.getTablePath(), null, schema);
    }

    /**
     * Parser that loads the schema from {@code schemas} and writes schema records back.
     */
    public static CdcParser of(TableSchemaSource schemas, String tablePath) {
        if (schemas == null) {
            throw new IllegalArgumentException("schemas is required");
        }
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath is required");
        }
        return new CdcParser(tablePath, schemas, null);
    }

    public String getTablePath() {
        return tablePath;
    }

    /**
     * @return schema currently used for data records, or {@code null} until one is known
     */
    @Nullable
    public CdcTableSchema getSchema() {
        return schema;
    }

    /**
     * Detects JSON or binary and decodes every record in the block.
     */
    public List<CdcRecord> parse(byte[] payload) {
        if (payload == null) {
            throw new CdcParseException("CDC payload is null", 0);
        }
        return parse(payload, 0, payload.length);
    }

    public List<CdcRecord> parse(byte[] payload, int offset, int length) {
        if (payload == null) {
            throw new CdcParseException("CDC payload is null", 0);
        }
        if (offset < 0 || length < 0 || offset + length > payload.length) {
            throw new CdcParseException("CDC payload slice is out of range", Math.max(offset, 0));
        }
        int start = skipSpace(payload, offset, length);
        if (start >= offset + length) {
            throw new CdcParseException("CDC payload is empty", offset);
        }
        if (payload[start] == '{' || payload[start] == '[') {
            return parseJson(new String(payload, offset, length, StandardCharsets.UTF_8));
        }
        return BinaryCdcDecoder.decodeAll(payload, offset, length, state);
    }

    public List<CdcRecord> parseJson(String json) {
        if (json == null) {
            throw new CdcParseException("CDC JSON payload is null", 0);
        }
        JsonStreamParser stream = new JsonStreamParser(json);
        if (!stream.hasNext()) {
            throw new CdcParseException("CDC JSON payload is empty", 0);
        }
        List<CdcRecord> records = new ArrayList<CdcRecord>();
        while (stream.hasNext()) {
            JsonElement element = stream.next();
            if (element.isJsonArray()) {
                for (JsonElement item : element.getAsJsonArray()) {
                    records.add(decodeJson(item));
                }
            } else {
                records.add(decodeJson(element));
            }
        }
        return records;
    }

    /**
     * Decodes a batch of topic messages in order. Schema records apply to later messages.
     */
    public List<CdcRecord> parseMessages(Iterable<byte[]> messages) {
        List<CdcRecord> records = new ArrayList<CdcRecord>();
        for (byte[] message : messages) {
            records.addAll(parse(message));
        }
        return records;
    }

    private CdcTableSchema requiredSchema() {
        if (schema != null) {
            return schema;
        }
        if (schemas == null) {
            throw new CdcParseException("CDC table schema is not set for " + tablePath, 0);
        }
        schema = schemas.describe(tablePath);
        if (schema == null) {
            throw new CdcParseException("table schema is not available for " + tablePath, 0);
        }
        return schema;
    }

    private void replaceSchema(CdcTableSchema updated) {
        schema = updated;
        if (schemas != null) {
            schemas.applySchemaChange(tablePath, updated);
        }
    }

    private CdcRecord decodeJson(JsonElement element) {
        if (!element.isJsonObject()) {
            throw new CdcParseException("CDC JSON record is not an object", 0);
        }
        JsonObject object = element.getAsJsonObject();
        CdcTableSchema current = object.has("key") ? requiredSchema() : schema;
        CdcRecord record = JsonCdcDecoder.decode(object, current, tablePath);
        if (record.getKind() == CdcRecord.Kind.SCHEMA) {
            replaceSchema(record.getTableSchema());
        }
        return record;
    }

    private static int skipSpace(byte[] payload, int offset, int length) {
        int index = offset;
        int end = offset + length;
        if (length >= 3 && payload[offset] == (byte) 0xef
                && payload[offset + 1] == (byte) 0xbb && payload[offset + 2] == (byte) 0xbf) {
            index += 3;
        }
        while (index < end) {
            byte current = payload[index];
            if (current != ' ' && current != '\n' && current != '\r' && current != '\t') {
                break;
            }
            index++;
        }
        return index;
    }

    private final class State implements BinaryCdcDecoder.SchemaState {
        @Override
        public String tablePath() {
            return tablePath;
        }

        @Override
        public CdcTableSchema schema() {
            return requiredSchema();
        }

        @Override
        public void replace(CdcTableSchema updated) {
            replaceSchema(updated);
        }
    }
}
