package tech.ydb.cdc;

import java.util.Objects;

final class CountingSource implements TableSchemaSource {
    private final CdcTableSchema schema;
    int calls;

    CountingSource(CdcTableSchema schema) {
        this.schema = schema;
    }

    @Override
    public CdcTableSchema describe(String tablePath) {
        calls++;
        if ("/db/missing".equals(tablePath) || schema == null) {
            return null;
        }
        return schema;
    }

    @Override
    public void applySchemaChange(String tablePath, CdcTableSchema updated) {
        Objects.requireNonNull(tablePath, "tablePath");
        Objects.requireNonNull(updated, "updated");
    }
}
