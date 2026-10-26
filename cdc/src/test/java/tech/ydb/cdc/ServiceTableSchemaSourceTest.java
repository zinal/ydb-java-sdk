package tech.ydb.cdc;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;

public class ServiceTableSchemaSourceTest {

    @Test
    public void fallbackIsStoredAndNotAskedAgain() {
        CdcTableSchema live = CdcTableSchema.builder("/db/t")
                .schemaVersion(4)
                .key("id", PrimitiveType.Uint64)
                .column("name", OptionalType.of(PrimitiveType.Text))
                .build();
        MemoryRows rows = new MemoryRows();
        CountingSource fallback = new CountingSource(live);
        ServiceTableSchemaSource source = new ServiceTableSchemaSource(rows, fallback);

        Assert.assertEquals(live, source.describe("/db/t"));
        Assert.assertEquals(live, source.describe("/db/t"));
        Assert.assertEquals(1, fallback.calls);
        Assert.assertNull(source.describe("/db/missing"));
        Assert.assertEquals(2, fallback.calls);
    }

    @Test
    public void schemaChangeKeepsTheNewestVersion() {
        MemoryRows rows = new MemoryRows();
        CountingSource fallback = new CountingSource(null);
        ServiceTableSchemaSource source = new ServiceTableSchemaSource(rows, fallback);
        CdcTableSchema newer = CdcTableSchema.builder("/db/t")
                .schemaVersion(5)
                .virtualTimestamp(9, 1)
                .key("id", PrimitiveType.Uint64)
                .build();
        CdcTableSchema older = CdcTableSchema.builder("/db/t")
                .schemaVersion(1)
                .key("id", PrimitiveType.Int32)
                .build();
        source.applySchemaChange("/db/t", newer);
        source.applySchemaChange("/db/t", older);
        Assert.assertEquals(newer, source.describe("/db/t"));
        Assert.assertEquals(0, fallback.calls);

        CdcTableSchema sameVersion = CdcTableSchema.builder("/db/t")
                .schemaVersion(5)
                .virtualTimestamp(9, 2)
                .key("id", PrimitiveType.Uint64)
                .column("extra", PrimitiveType.Text)
                .build();
        source.applySchemaChange("/db/t", sameVersion);
        Assert.assertEquals(sameVersion, source.describe("/db/t"));
    }

    @Test
    public void describeTableSnapshotAndQuoting() {
        TableDescription description = TableDescription.newBuilder()
                .addNonnullColumn("id", PrimitiveType.Uint64)
                .addNullableColumn("name", PrimitiveType.Text, "default")
                .setPrimaryKeys("id")
                .addGlobalIndex("by_name", Collections.singletonList("name"))
                .build();
        CdcTableSchema schema = CdcTableSchema.fromTableDescription("/db/t", description);
        Assert.assertEquals("Uint64", schema.column("id").getTypeName());
        Assert.assertEquals("Optional<Utf8>", schema.column("name").getTypeName());
        Assert.assertEquals("default", schema.column("name").getFamily());
        Assert.assertEquals("GlobalSync", schema.getIndexes().get("by_name").getTypeName());

        CdcTableSchema restored = CdcTableSchema.fromChangeDocument("/db/t", schema.toJson());
        Assert.assertEquals(schema, restored);
        Assert.assertEquals("`a``b`", YdbSchemaRows.quote("a`b"));
    }
}
