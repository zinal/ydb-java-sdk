package tech.ydb.cdc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tech.ydb.table.values.Value;

/**
 * Decodes records of the binary changefeed format (version 1, little-endian).
 * A buffer may contain one record or several records concatenated: each record
 * is delimited by its own header and cell sizes.
 */
final class BinaryCdcDecoder {
    static final int VERSION = 1;
    static final int KIND_DATA = 1;
    static final int KIND_HEARTBEAT = 2;
    static final int KIND_SCHEMA = 3;

    static final int OP_NONE = 0;
    static final int OP_UPSERT = 1;
    static final int OP_RESET = 2;
    static final int OP_ERASE = 3;

    static final int FLAG_TS = 1;
    static final int FLAG_WIDE = 1 << 1;
    static final int FLAG_OLD = 1 << 2;
    static final int FLAG_NEW = 1 << 3;
    static final int FLAG_USER = 1 << 4;
    static final int FLAG_TRACE = 1 << 5;
    static final int FLAG_NULL_KEYS = 1 << 6;
    static final int KNOWN_FLAGS = FLAG_TS | FLAG_WIDE | FLAG_OLD | FLAG_NEW | FLAG_USER | FLAG_TRACE
            | FLAG_NULL_KEYS;

    private BinaryCdcDecoder() {
    }

    static List<CdcRecord> decodeAll(byte[] payload, int offset, int length, SchemaState state) {
        if (length < 0 || offset < 0 || offset + length > payload.length) {
            throw new CdcParseException("CDC payload slice is out of range", Math.max(offset, 0));
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        List<CdcRecord> records = new ArrayList<CdcRecord>();
        while (buffer.hasRemaining()) {
            records.add(decodeOne(buffer, state, buffer.position()));
        }
        return records;
    }

    private static CdcRecord decodeOne(ByteBuffer buffer, SchemaState state, int offset) {
        Header header = header(buffer, offset);
        if (header.kind == KIND_SCHEMA) {
            CdcRecord record = schemaRecord(buffer, header, state.tablePath(), offset);
            state.replace(record.getTableSchema());
            return record;
        }
        if (header.kind == KIND_HEARTBEAT) {
            return heartbeat(buffer, header, offset);
        }
        return data(buffer, header, state.schema(), offset);
    }

    private static CdcRecord schemaRecord(ByteBuffer buffer, Header header, String tablePath, int offset) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new CdcParseException("truncated binary CDC schema record", offset);
        }
        long size = Integer.toUnsignedLong(buffer.getInt());
        if (size > buffer.remaining()) {
            throw new CdcParseException("truncated binary CDC schema record", offset);
        }
        byte[] json = new byte[(int) size];
        buffer.get(json);
        String user = header.has(FLAG_USER) ? text(buffer, offset, "user SID") : null;
        String trace = header.has(FLAG_TRACE) ? text(buffer, offset, "trace id") : null;
        CdcTableSchema parsed = CdcTableSchema.fromChangeDocument(
                tablePath, new String(json, StandardCharsets.UTF_8));
        CdcRecord.Builder builder = CdcRecord.builder(CdcRecord.Kind.SCHEMA).tableSchema(parsed);
        if (header.hasTs) {
            builder.virtualTimestamp(header.step, header.txId);
        } else if (parsed.getStep() != null && parsed.getTxId() != null) {
            builder.virtualTimestamp(parsed.getStep().longValue(), parsed.getTxId().longValue());
        }
        if (user != null) {
            builder.user(user);
        }
        if (trace != null) {
            builder.traceId(trace);
        }
        return builder.build();
    }

    private static CdcRecord heartbeat(ByteBuffer buffer, Header header, int offset) {
        if (!header.hasTs) {
            throw new CdcParseException("binary CDC barrier has no virtual timestamp", offset);
        }
        String user = header.has(FLAG_USER) ? text(buffer, offset, "user SID") : null;
        String trace = header.has(FLAG_TRACE) ? text(buffer, offset, "trace id") : null;
        CdcRecord.Builder builder = CdcRecord.builder(CdcRecord.Kind.HEARTBEAT)
                .virtualTimestamp(header.step, header.txId);
        if (user != null) {
            builder.user(user);
        }
        if (trace != null) {
            builder.traceId(trace);
        }
        return builder.build();
    }

    private static CdcRecord data(ByteBuffer buffer, Header header, CdcTableSchema schema, int offset) {
        if (header.keyCount != schema.getPrimaryKey().size()) {
            throw new CdcParseException("binary CDC key column count mismatch: record " + header.keyCount
                    + ", schema " + schema.getPrimaryKey().size(), offset);
        }
        boolean images = header.has(FLAG_OLD) || header.has(FLAG_NEW);
        if (images && header.valueCount != 0) {
            throw new CdcParseException("binary CDC image record also has value cells", offset);
        }
        List<CdcCell> key = keys(buffer, header, schema, offset);
        Map<String, Value<?>> values = indexed(buffer, header.valueCount, header, schema, offset);
        Map<String, Value<?>> oldImage = header.has(FLAG_OLD)
                ? indexed(buffer, header.oldCount, header, schema, offset) : null;
        Map<String, Value<?>> newImage = header.has(FLAG_NEW)
                ? indexed(buffer, header.newCount, header, schema, offset) : null;
        String user = header.has(FLAG_USER) ? text(buffer, offset, "user SID") : null;
        String trace = header.has(FLAG_TRACE) ? text(buffer, offset, "trace id") : null;

        CdcRecord.Builder builder = CdcRecord.builder(CdcRecord.Kind.DATA)
                .operation(operation(header.op, offset))
                .key(key);
        if (header.op == OP_UPSERT) {
            builder.updateColumns(images ? new LinkedHashMap<String, Value<?>>() : values);
        } else if (header.op == OP_RESET) {
            builder.resetColumns(images ? new LinkedHashMap<String, Value<?>>() : values);
        } else if (header.valueCount != 0) {
            throw new CdcParseException("binary CDC erase record has value cells", offset);
        }
        if (oldImage != null) {
            builder.oldImage(oldImage);
        }
        if (newImage != null) {
            builder.newImage(newImage);
        }
        if (header.hasTs) {
            builder.virtualTimestamp(header.step, header.txId);
        }
        if (user != null) {
            builder.user(user);
        }
        if (trace != null) {
            builder.traceId(trace);
        }
        return builder.build();
    }

    private static List<CdcCell> keys(ByteBuffer buffer, Header header, CdcTableSchema schema, int offset) {
        List<CdcCell> cells = new ArrayList<CdcCell>(header.keyCount);
        for (int i = 0; i < header.keyCount; i++) {
            boolean isNull = false;
            if (header.has(FLAG_NULL_KEYS)) {
                int marker = readU8(buffer, offset);
                if (marker > 1) {
                    throw new CdcParseException("binary CDC key null marker is not 0 or 1", offset);
                }
                isNull = marker == 1;
            }
            String name = schema.getPrimaryKey().get(i);
            CdcColumn column = schema.column(name);
            cells.add(new CdcCell(name, CdcValues.fromCell(buffer, column.getType(), isNull, offset)));
        }
        return cells;
    }

    private static Map<String, Value<?>> indexed(ByteBuffer buffer, int count, Header header, CdcTableSchema schema,
            int offset) {
        LinkedHashMap<String, Value<?>> columns = new LinkedHashMap<String, Value<?>>();
        List<CdcColumn> values = schema.getValueColumns();
        boolean wide = header.has(FLAG_WIDE);
        for (int i = 0; i < count; i++) {
            int wire = wide ? readU16(buffer, offset) : readU8(buffer, offset);
            int nullBit = wide ? 0x8000 : 0x80;
            int mask = wide ? 0x7fff : 0x7f;
            boolean isNull = (wire & nullBit) != 0;
            int index = wire & mask;
            if (index >= values.size()) {
                throw new CdcParseException("binary CDC column index " + index
                        + " is outside the schema of " + values.size() + " value columns", offset);
            }
            CdcColumn column = values.get(index);
            if (columns.containsKey(column.getName())) {
                throw new CdcParseException("binary CDC column index " + index + " is repeated", offset);
            }
            columns.put(column.getName(), CdcValues.fromCell(buffer, column.getType(), isNull, offset));
        }
        return columns;
    }

    private static Header header(ByteBuffer buffer, int offset) {
        if (buffer.remaining() < 8) {
            throw new CdcParseException("truncated binary CDC record", offset);
        }
        Header header = new Header();
        int version = buffer.get() & 0xff;
        if (version != VERSION) {
            throw new CdcParseException("unsupported binary CDC version: " + version, offset);
        }
        header.kind = buffer.get() & 0xff;
        header.op = checkOp(buffer.get() & 0xff, offset);
        header.flags = buffer.get() & 0xff;
        if ((header.flags & ~KNOWN_FLAGS) != 0) {
            throw new CdcParseException("unknown binary CDC flags", offset);
        }
        if (header.kind != KIND_DATA && header.kind != KIND_HEARTBEAT && header.kind != KIND_SCHEMA) {
            throw new CdcParseException("unknown binary CDC kind: " + header.kind, offset);
        }
        header.keyCount = buffer.getShort() & 0xffff;
        header.valueCount = buffer.getShort() & 0xffff;
        if (header.has(FLAG_NULL_KEYS) && (header.kind != KIND_DATA || header.keyCount == 0)) {
            throw new CdcParseException("binary CDC null key flag requires key cells", offset);
        }
        if (header.has(FLAG_OLD)) {
            header.oldCount = readU16(buffer, offset);
        }
        if (header.has(FLAG_NEW)) {
            header.newCount = readU16(buffer, offset);
        }
        if (header.kind != KIND_DATA
                && (header.keyCount != 0 || header.valueCount != 0 || header.oldCount != 0 || header.newCount != 0)) {
            throw new CdcParseException("binary CDC control record has cells", offset);
        }
        if (header.has(FLAG_TS)) {
            if (buffer.remaining() < Long.BYTES * 2) {
                throw new CdcParseException("truncated binary CDC virtual timestamp", offset);
            }
            header.step = buffer.getLong();
            header.txId = buffer.getLong();
            header.hasTs = true;
        }
        return header;
    }

    private static int checkOp(int op, int offset) {
        if (op != OP_NONE && op != OP_UPSERT && op != OP_RESET && op != OP_ERASE) {
            throw new CdcParseException("unknown binary CDC operation: " + op, offset);
        }
        return op;
    }

    private static CdcRecord.Operation operation(int op, int offset) {
        switch (op) {
            case OP_UPSERT:
                return CdcRecord.Operation.UPSERT;
            case OP_RESET:
                return CdcRecord.Operation.RESET;
            case OP_ERASE:
                return CdcRecord.Operation.ERASE;
            case OP_NONE:
            default:
                throw new CdcParseException("binary CDC data record has no operation", offset);
        }
    }

    private static String text(ByteBuffer buffer, int offset, String what) {
        if (buffer.remaining() < Short.BYTES) {
            throw new CdcParseException("truncated binary CDC " + what, offset);
        }
        int size = buffer.getShort() & 0xffff;
        if (buffer.remaining() < size) {
            throw new CdcParseException("truncated binary CDC " + what, offset);
        }
        byte[] raw = new byte[size];
        buffer.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }

    private static int readU8(ByteBuffer buffer, int offset) {
        if (!buffer.hasRemaining()) {
            throw new CdcParseException("truncated binary CDC record", offset);
        }
        return buffer.get() & 0xff;
    }

    private static int readU16(ByteBuffer buffer, int offset) {
        if (buffer.remaining() < Short.BYTES) {
            throw new CdcParseException("truncated binary CDC record", offset);
        }
        return buffer.getShort() & 0xffff;
    }

    interface SchemaState {
        String tablePath();

        CdcTableSchema schema();

        void replace(CdcTableSchema schema);
    }

    private static final class Header {
        private int kind;
        private int op;
        private int flags;
        private int keyCount;
        private int valueCount;
        private int oldCount;
        private int newCount;
        private boolean hasTs;
        private long step;
        private long txId;

        private boolean has(int flag) {
            return (flags & flag) != 0;
        }
    }
}
