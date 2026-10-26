package tech.ydb.cdc;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.DecimalValue;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;

public class CdcParserTest {
    private static final DecimalType DECIMAL = DecimalType.of(22, 9);
    private static final Instant INSTANT = Instant.parse("2022-12-12T00:00:00.000000Z");

    @Test
    public void jsonAndBinaryUpsertMatch() {
        CdcTableSchema schema = sampleSchema();
        String json = "{"
                + "\"key\":[1],"
                + "\"update\":{"
                + "\"amount\":\"1.50\","
                + "\"flag\":true,"
                + "\"name\":\"a\","
                + "\"payload\":\"YWI=\","
                + "\"ts\":\"2022-12-12T00:00:00.000000Z\""
                + "},"
                + "\"ts\":[10,20],"
                + "\"user\":\"user@x\","
                + "\"traceId\":\"abc\""
                + "}";
        CdcParser parser = CdcParser.of(schema);
        CdcRecord fromJson = parser.parse(json.getBytes(java.nio.charset.StandardCharsets.UTF_8)).get(0);
        CdcRecord fromBinary = parser.parse(sampleUpsert()).get(0);
        Assert.assertEquals(fromJson, fromBinary);
        Assert.assertEquals(CdcRecord.Operation.UPSERT, fromJson.getOperation());
        Assert.assertEquals(Long.valueOf(10L), fromJson.getStep());
        Assert.assertEquals("user@x", fromJson.getUser());
        Assert.assertEquals("abc", fromJson.getTraceId());
        Assert.assertFalse(fromJson.rowAfter().containsKey("id"));
        Assert.assertEquals(PrimitiveValue.newText("a"), fromJson.rowAfter().get("name").asOptional().get());
    }

    @Test
    public void nullOptionalAndEraseAndHeartbeatMatch() {
        CdcTableSchema schema = CdcTableSchema.builder("/db/t")
                .key("id", PrimitiveType.Uint64)
                .column("name", OptionalType.of(PrimitiveType.Text))
                .build();
        CdcParser parser = CdcParser.of(schema);

        String nullJson = "{\"key\":[1],\"update\":{\"name\":null}}";
        byte[] nullBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT, 0, 1, 1)
                .u64(1)
                .u8(0x80)
                .toByteArray();
        Assert.assertEquals(parser.parseJson(nullJson).get(0), parser.parse(nullBinary).get(0));

        String eraseJson = "{\"key\":[1],\"erase\":{}}";
        byte[] eraseBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_ERASE, 0, 1, 0)
                .u64(1)
                .toByteArray();
        CdcRecord erase = parser.parseJson(eraseJson).get(0);
        Assert.assertEquals(erase, parser.parse(eraseBinary).get(0));
        Assert.assertTrue(erase.isErase());
        Assert.assertNull(erase.rowAfter());

        String heartJson = "{\"resolved\":[3,4]}";
        byte[] heartBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_HEARTBEAT, BinaryCdcDecoder.OP_NONE, BinaryCdcDecoder.FLAG_TS, 0, 0)
                .u64(3)
                .u64(4)
                .toByteArray();
        CdcRecord heart = parser.parseJson(heartJson).get(0);
        Assert.assertEquals(heart, parser.parse(heartBinary).get(0));
        Assert.assertEquals(CdcRecord.Kind.HEARTBEAT, heart.getKind());
        Assert.assertEquals(Long.valueOf(4L), heart.getTxId());
    }

    @Test
    public void imagesResetWideIndexAndNullKeyMatch() {
        CdcTableSchema schema = CdcTableSchema.builder("/db/t")
                .key("id", PrimitiveType.Uint64)
                .column("name", OptionalType.of(PrimitiveType.Text))
                .build();
        CdcParser parser = CdcParser.of(schema);
        String imageJson = "{\"key\":[1],\"update\":{},\"oldImage\":{\"name\":null},"
                + "\"newImage\":{\"name\":\"n\"},\"ts\":[1,2]}";
        byte[] imageBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT,
                        BinaryCdcDecoder.FLAG_TS | BinaryCdcDecoder.FLAG_OLD | BinaryCdcDecoder.FLAG_NEW, 1, 0)
                .u16(1)
                .u16(1)
                .u64(1)
                .u64(2)
                .u64(1)
                .u8(0x80)
                .u8(0)
                .textCell("n")
                .toByteArray();
        CdcRecord image = parser.parseJson(imageJson).get(0);
        Assert.assertEquals(image, parser.parse(imageBinary).get(0));
        Assert.assertEquals(PrimitiveValue.newUint64(1), image.rowAfter().get("id"));
        Assert.assertFalse(image.getOldImage().get("name").asOptional().isPresent());
        Assert.assertEquals(PrimitiveValue.newText("n"), image.getNewImage().get("name").asOptional().get());

        String resetJson = "{\"key\":[1],\"reset\":{\"name\":\"r\"}}";
        byte[] resetBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_RESET, 0, 1, 1)
                .u64(1)
                .u8(0)
                .textCell("r")
                .toByteArray();
        CdcRecord reset = parser.parseJson(resetJson).get(0);
        Assert.assertEquals(reset, parser.parse(resetBinary).get(0));
        Assert.assertEquals(CdcRecord.Operation.RESET, reset.getOperation());

        byte[] wide = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT, BinaryCdcDecoder.FLAG_WIDE, 1, 1)
                .u64(1)
                .u16(0)
                .textCell("w")
                .toByteArray();
        Assert.assertEquals(parser.parseJson("{\"key\":[1],\"update\":{\"name\":\"w\"}}").get(0),
                parser.parse(wide).get(0));

        CdcTableSchema nullableKey = CdcTableSchema.builder("/db/t")
                .key("id", OptionalType.of(PrimitiveType.Uint64))
                .build();
        byte[] nullKey = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_ERASE, BinaryCdcDecoder.FLAG_NULL_KEYS, 1, 0)
                .u8(1)
                .toByteArray();
        Assert.assertEquals(CdcParser.of(nullableKey).parseJson("{\"key\":[null],\"erase\":{}}").get(0),
                CdcParser.of(nullableKey).parse(nullKey).get(0));
    }

    @Test
    public void temporalNumbersAndUnsignedKey() {
        CdcTableSchema schema = CdcTableSchema.builder("/db/t")
                .key("id", PrimitiveType.Uint64)
                .column("day", PrimitiveType.Date)
                .column("span", PrimitiveType.Interval)
                .column("day32", PrimitiveType.Date32)
                .build();
        String json = "{\"key\":[18446744073709551615],\"update\":{"
                + "\"day\":\"2022-02-22T00:00:00.000000Z\",\"span\":-5,\"day32\":10}}";
        long days = LocalDate.of(2022, 2, 22).toEpochDay();
        byte[] binary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT, 0, 1, 3)
                .u64(-1L)
                .u8(0)
                .u16((int) days)
                .u8(1)
                .u32(10)
                .u8(2)
                .u64(-5L)
                .toByteArray();
        CdcRecord record = CdcParser.of(schema).parseJson(json).get(0);
        Assert.assertEquals(record, CdcParser.of(schema).parse(binary).get(0));
        Assert.assertEquals(PrimitiveValue.newDate(days), record.getUpdateColumns().get("day"));
        Assert.assertEquals(PrimitiveValue.newInterval(-5), record.getUpdateColumns().get("span"));
        Assert.assertEquals(PrimitiveValue.newDate32(10), record.getUpdateColumns().get("day32"));
    }

    @Test
    public void schemaRecordUpdatesFollowingRecordsAndServiceTable() {
        CdcTableSchema initial = CdcTableSchema.builder("/db/t")
                .schemaVersion(1)
                .key("id", PrimitiveType.Uint64)
                .build();
        MemoryRows rows = new MemoryRows();
        CountingSource fallback = new CountingSource(initial);
        ServiceTableSchemaSource source = new ServiceTableSchemaSource(rows, fallback);
        String block = "["
                + "{\"tableChanges\":[{\"table\":{\"schemaVersion\":2,"
                + "\"primaryKeyColumnNames\":[\"id\"],"
                + "\"columns\":{\"id\":{\"type\":\"Uint64\",\"family\":\"default\"},"
                + "\"name\":{\"type\":\"Optional<Utf8>\",\"family\":\"default\"}},"
                + "\"indexes\":{}}}],\"ts\":[5,1]},"
                + "{\"key\":[7],\"update\":{\"name\":\"z\"},\"ts\":[5,2]}"
                + "]";
        CdcParser parser = CdcParser.of(source, "/db/t");
        java.util.List<CdcRecord> records = parser.parseJson(block);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals(CdcRecord.Kind.SCHEMA, records.get(0).getKind());
        Assert.assertEquals(2L, records.get(0).getTableSchema().getSchemaVersion());
        Assert.assertEquals(PrimitiveValue.newText("z"),
                records.get(1).getUpdateColumns().get("name").asOptional().get());
        Assert.assertEquals(0, fallback.calls);
        Assert.assertEquals(2L, source.describe("/db/t").getSchemaVersion());

        byte[] schemaBinary = schemaBinary(records.get(0).getTableSchema());
        byte[] dataBinary = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT, BinaryCdcDecoder.FLAG_TS, 1, 1)
                .u64(5)
                .u64(2)
                .u64(7)
                .u8(0)
                .textCell("z")
                .toByteArray();
        BinaryBytes joined = new BinaryBytes().raw(schemaBinary).raw(dataBinary);
        CdcParser binaryParser = CdcParser.of(initial);
        java.util.List<CdcRecord> binaryRecords = binaryParser.parse(joined.toByteArray());
        Assert.assertEquals(records.get(1), binaryRecords.get(1));
    }

    @Test
    public void ndjsonMessagesAndRejections() {
        CdcTableSchema schema = CdcTableSchema.builder("/db/t").key("id", PrimitiveType.Uint64).build();
        CdcParser parser = CdcParser.of(schema);
        java.util.List<CdcRecord> ndjson = parser.parseJson(
                "{\"key\":[1],\"erase\":{}}\n{\"key\":[2],\"erase\":{}}\n");
        Assert.assertEquals(2, ndjson.size());
        Assert.assertEquals(PrimitiveValue.newUint64(2), ndjson.get(1).getKey().get(0).getValue());

        byte[] first = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_ERASE, 0, 1, 0)
                .u64(1)
                .toByteArray();
        byte[] second = new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_ERASE, 0, 1, 0)
                .u64(2)
                .toByteArray();
        java.util.List<CdcRecord> messages = parser.parseMessages(Arrays.asList(first, second));
        Assert.assertEquals(ndjson, messages);

        try {
            parser.parse(new byte[] {2, 0, 0, 0, 0, 0, 0, 0});
            Assert.fail("unsupported version must fail");
        } catch (CdcParseException ex) {
            Assert.assertTrue(ex.getMessage().contains("version"));
        }
        try {
            parser.parseJson("{\"key\":[1],\"update\":{},\"erase\":{}}");
            Assert.fail("two operations must fail");
        } catch (CdcParseException ex) {
            Assert.assertTrue(ex.getMessage().contains("update"));
        }
    }

    private static CdcTableSchema sampleSchema() {
        return CdcTableSchema.builder("/db/t")
                .schemaVersion(1)
                .key("id", PrimitiveType.Uint64)
                .column("amount", OptionalType.of(DECIMAL))
                .column("flag", OptionalType.of(PrimitiveType.Bool))
                .column("name", OptionalType.of(PrimitiveType.Text))
                .column("payload", OptionalType.of(PrimitiveType.Bytes))
                .column("ts", OptionalType.of(PrimitiveType.Timestamp))
                .build();
    }

    private static byte[] sampleUpsert() {
        DecimalValue amount = DECIMAL.newValue("1.50");
        long micros = INSTANT.getEpochSecond() * 1000000L + INSTANT.getNano() / 1000;
        int flags = BinaryCdcDecoder.FLAG_TS | BinaryCdcDecoder.FLAG_USER | BinaryCdcDecoder.FLAG_TRACE;
        return new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_DATA, BinaryCdcDecoder.OP_UPSERT, flags, 1, 5)
                .u64(10)
                .u64(20)
                .u64(1)
                .u8(0)
                .u64(amount.getLow())
                .u64(amount.getHigh())
                .u8(1)
                .u8(1)
                .u8(2)
                .textCell("a")
                .u8(3)
                .u32(2)
                .raw(new byte[] {'a', 'b'})
                .u8(4)
                .u64(micros)
                .u16(6)
                .raw("user@x".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .u16(3)
                .raw("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toByteArray();
    }

    private static byte[] schemaBinary(CdcTableSchema schema) {
        String json = "{\"tableChanges\":[{\"table\":" + schema.toJson() + "}],\"ts\":[5,1]}";
        byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new BinaryBytes()
                .header(BinaryCdcDecoder.KIND_SCHEMA, BinaryCdcDecoder.OP_NONE, 0, 0, 0)
                .u32(body.length)
                .raw(body)
                .toByteArray();
    }

    @Test
    public void valueColumnOrderFollowsUtf8Names() {
        CdcTableSchema schema = sampleSchema();
        Assert.assertEquals(Arrays.asList("amount", "flag", "name", "payload", "ts"),
                names(schema));
        Assert.assertEquals(Collections.singletonList("id"), schema.getPrimaryKey());
    }

    private static java.util.List<String> names(CdcTableSchema schema) {
        java.util.List<String> names = new java.util.ArrayList<String>();
        for (CdcColumn column : schema.getValueColumns()) {
            names.add(column.getName());
        }
        return names;
    }
}
