package tech.ydb.cdc;

import javax.annotation.Nullable;

/**
 * Storage of one schema snapshot per source table.
 */
interface SchemaRows {
    @Nullable
    CdcTableSchema read(String tablePath);

    void write(String tablePath, CdcTableSchema schema);
}
