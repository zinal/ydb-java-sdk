package tech.ydb.cdc;

import java.util.Objects;
import java.util.concurrent.CompletionException;

import javax.annotation.Nullable;

import tech.ydb.core.Result;
import tech.ydb.core.StatusCode;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;

/**
 * Reads the current schema with {@code DescribeTable} on the source table.
 * Schema records are not stored: the next describe sees the altered table.
 */
public final class DescribeTableSchemaSource implements TableSchemaSource {
    private final SessionRetryContext retry;

    public DescribeTableSchemaSource(TableClient tableClient) {
        this(SessionRetryContext.create(Objects.requireNonNull(tableClient, "tableClient")).build());
    }

    public DescribeTableSchemaSource(SessionRetryContext retry) {
        this.retry = Objects.requireNonNull(retry, "retry");
    }

    @Override
    @Nullable
    public CdcTableSchema describe(String tablePath) {
        Result<TableDescription> described;
        try {
            described = retry.supplyResult(session -> session.describeTable(tablePath)).join();
        } catch (CompletionException ex) {
            throw new IllegalStateException("failed to describe " + tablePath, ex);
        }
        if (!described.isSuccess()) {
            if (described.getStatus().getCode() == StatusCode.SCHEME_ERROR) {
                return null;
            }
            described.getStatus().expectSuccess("describe " + tablePath);
        }
        return CdcTableSchema.fromTableDescription(tablePath, described.getValue());
    }

    @Override
    public void applySchemaChange(String tablePath, CdcTableSchema schema) {
        // The source table already has this schema. The next describe reads it.
        Objects.requireNonNull(tablePath, "tablePath");
        Objects.requireNonNull(schema, "schema");
    }
}
