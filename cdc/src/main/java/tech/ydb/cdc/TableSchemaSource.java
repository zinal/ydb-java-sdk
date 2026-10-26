package tech.ydb.cdc;

import javax.annotation.Nullable;

/**
 * Current schema of source tables, used to decode CDC records.
 * <p>
 * Two implementations are provided: {@link DescribeTableSchemaSource} reads
 * {@code DescribeTable} of the source table, and {@link ServiceTableSchemaSource}
 * keeps the latest snapshot in a service table, falling back to describe when
 * that snapshot is missing. A schema record in the changefeed is applied with
 * {@link #applySchemaChange(String, CdcTableSchema)}.
 */
public interface TableSchemaSource {

    /**
     * @param tablePath path of the source table
     * @return the schema to use for the next records, or {@code null} when it is unknown
     */
    @Nullable
    CdcTableSchema describe(String tablePath);

    /**
     * Remember a schema snapshot observed in the changefeed.
     * Implementations that always read the live table may ignore the call.
     *
     * @param tablePath path of the source table the snapshot belongs to
     * @param schema snapshot carried by the schema record
     */
    void applySchemaChange(String tablePath, CdcTableSchema schema);
}
