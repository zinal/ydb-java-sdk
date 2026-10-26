package tech.ydb.cdc;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import javax.annotation.Nullable;

import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Value;

/**
 * Service table with one row per source table.
 * <pre>
 * CREATE TABLE service (
 *     table_path Utf8 NOT NULL,
 *     schema_version Uint64 NOT NULL,
 *     schema_json Json NOT NULL,
 *     step Uint64,
 *     tx_id Uint64,
 *     PRIMARY KEY (table_path)
 * );
 * </pre>
 */
final class YdbSchemaRows implements SchemaRows {
    private static final OptionalType UINT64_OPTIONAL = OptionalType.of(PrimitiveType.Uint64);

    private final SessionRetryContext retry;
    private final String serviceTable;
    private final String quoted;
    private boolean ready;

    YdbSchemaRows(SessionRetryContext retry, String serviceTable) {
        this.retry = Objects.requireNonNull(retry, "retry");
        if (serviceTable == null || serviceTable.isEmpty()) {
            throw new IllegalArgumentException("service table path is required");
        }
        this.serviceTable = serviceTable;
        this.quoted = quote(serviceTable);
    }

    @Override
    @Nullable
    public CdcTableSchema read(String tablePath) {
        ensure();
        String query = "DECLARE $table_path AS Utf8; "
                + "SELECT schema_version, schema_json, step, tx_id FROM " + quoted
                + " WHERE table_path = $table_path";
        DataQueryResult result = join(retry.supplyResult(session -> session.executeDataQuery(
                query, TxControl.onlineRo(), Params.of("$table_path", PrimitiveValue.newText(tablePath)))));
        ResultSetReader reader = result.getResultSet(0);
        if (!reader.next()) {
            return null;
        }
        String json = reader.getColumn("schema_json").getJson();
        return CdcTableSchema.fromChangeDocument(tablePath, json);
    }

    @Override
    public void write(String tablePath, CdcTableSchema schema) {
        ensure();
        String query = "DECLARE $table_path AS Utf8; "
                + "DECLARE $schema_version AS Uint64; "
                + "DECLARE $schema_json AS Json; "
                + "DECLARE $step AS Uint64?; "
                + "DECLARE $tx_id AS Uint64?; "
                + "UPSERT INTO " + quoted
                + " (table_path, schema_version, schema_json, step, tx_id) "
                + "VALUES ($table_path, $schema_version, $schema_json, $step, $tx_id)";
        Params params = Params.create(5)
                .put("$table_path", PrimitiveValue.newText(tablePath))
                .put("$schema_version", PrimitiveValue.newUint64(schema.getSchemaVersion()))
                .put("$schema_json", PrimitiveValue.newJson(schema.toJson()))
                .put("$step", optionalUint64(schema.getStep()))
                .put("$tx_id", optionalUint64(schema.getTxId()));
        join(retry.supplyResult(session -> session.executeDataQuery(query, TxControl.serializableRw(), params)));
    }

    private void ensure() {
        if (ready) {
            return;
        }
        String ddl = "CREATE TABLE IF NOT EXISTS " + quoted
                + " (table_path Utf8 NOT NULL, schema_version Uint64 NOT NULL, "
                + "schema_json Json NOT NULL, step Uint64, tx_id Uint64, "
                + "PRIMARY KEY (table_path))";
        Status status;
        try {
            status = retry.supplyStatus(session -> session.executeSchemeQuery(ddl)).join();
        } catch (CompletionException ex) {
            throw new IllegalStateException("failed to create CDC schema table " + serviceTable, ex);
        }
        status.expectSuccess("create CDC schema table " + serviceTable);
        ready = true;
    }

    private static Value<?> optionalUint64(@Nullable Long value) {
        if (value == null) {
            return UINT64_OPTIONAL.emptyValue();
        }
        return OptionalValue.of(PrimitiveValue.newUint64(value.longValue()));
    }

    private static <T> T join(CompletableFuture<Result<T>> future) {
        try {
            return future.join().getValue();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() == null ? ex : ex.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw ex;
        }
    }

    static String quote(String path) {
        return "`" + path.replace("`", "``") + "`";
    }
}
