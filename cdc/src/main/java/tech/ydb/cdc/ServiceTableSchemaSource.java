package tech.ydb.cdc;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 * Stores the current schema of each source table in a service table.
 * When the row is missing, the schema is taken from {@link #fallback} and then stored.
 * A schema record from the changefeed overwrites the row unless a newer schema version
 * is already stored.
 */
public final class ServiceTableSchemaSource implements TableSchemaSource {
    private final SchemaRows rows;
    private final TableSchemaSource fallback;

    public ServiceTableSchemaSource(TableClient tableClient, String serviceTablePath, TableSchemaSource fallback) {
        this(SessionRetryContext.create(Objects.requireNonNull(tableClient, "tableClient")).build(),
                serviceTablePath, fallback);
    }

    public ServiceTableSchemaSource(SessionRetryContext retry, String serviceTablePath, TableSchemaSource fallback) {
        this(new YdbSchemaRows(retry, serviceTablePath), fallback);
    }

    ServiceTableSchemaSource(SchemaRows rows, TableSchemaSource fallback) {
        this.rows = Objects.requireNonNull(rows, "rows");
        this.fallback = Objects.requireNonNull(fallback, "fallback");
    }

    /**
     * Uses {@link DescribeTableSchemaSource} on the same retry context as the fallback.
     */
    public static ServiceTableSchemaSource describeFallback(TableClient tableClient, String serviceTablePath) {
        SessionRetryContext retry = SessionRetryContext.create(
                Objects.requireNonNull(tableClient, "tableClient")).build();
        return new ServiceTableSchemaSource(retry, serviceTablePath, new DescribeTableSchemaSource(retry));
    }

    @Override
    @Nullable
    public synchronized CdcTableSchema describe(String tablePath) {
        CdcTableSchema stored = rows.read(tablePath);
        if (stored != null) {
            return stored;
        }
        CdcTableSchema live = fallback.describe(tablePath);
        if (live != null) {
            rows.write(tablePath, live);
        }
        return live;
    }

    @Override
    public synchronized void applySchemaChange(String tablePath, CdcTableSchema schema) {
        Objects.requireNonNull(schema, "schema");
        CdcTableSchema stored = rows.read(tablePath);
        if (stored != null && Long.compareUnsigned(schema.getSchemaVersion(), stored.getSchemaVersion()) < 0) {
            return;
        }
        rows.write(tablePath, schema);
    }
}
