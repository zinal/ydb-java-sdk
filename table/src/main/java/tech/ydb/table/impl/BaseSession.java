package tech.ydb.table.impl;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.protobuf.Timestamp;
import tech.ydb.OperationProtos.Operation;
import tech.ydb.StatusCodesProtos.StatusIds;
import tech.ydb.ValueProtos;
import tech.ydb.common.CommonProtos;
import tech.ydb.core.Issue;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.rpc.StreamControl;
import tech.ydb.core.rpc.StreamObserver;
import tech.ydb.table.Session;
import tech.ydb.table.YdbTable;
import tech.ydb.table.YdbTable.ReadTableRequest;
import tech.ydb.table.YdbTable.ReadTableResponse;
import tech.ydb.table.description.ColumnFamily;
import tech.ydb.table.description.KeyBound;
import tech.ydb.table.description.KeyRange;
import tech.ydb.table.description.StoragePool;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.description.TableIndex;
import tech.ydb.table.query.DataQuery;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.ExplainDataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.impl.ProtoValueReaders;
import tech.ydb.table.rpc.TableRpc;
import tech.ydb.table.settings.AlterTableSettings;
import tech.ydb.table.settings.AutoPartitioningPolicy;
import tech.ydb.table.settings.BeginTxSettings;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.DeleteSessionSettings;
import tech.ydb.table.settings.CommitTxSettings;
import tech.ydb.table.settings.CopyTableSettings;
import tech.ydb.table.settings.CreateTableSettings;
import tech.ydb.table.settings.DescribeTableSettings;
import tech.ydb.table.settings.DropTableSettings;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.settings.ExplainDataQuerySettings;
import tech.ydb.table.settings.KeepAliveSessionSettings;
import tech.ydb.table.settings.PartitioningPolicy;
import tech.ydb.table.settings.PartitioningSettings;
import tech.ydb.table.settings.PrepareDataQuerySettings;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.settings.ReplicationPolicy;
import tech.ydb.table.settings.RollbackTxSettings;
import tech.ydb.table.settings.StoragePolicy;
import tech.ydb.table.settings.TtlSettings;
import tech.ydb.table.transaction.Transaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.utils.OperationParamUtils;
import tech.ydb.table.utils.RequestSettingsUtils;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.TupleValue;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.proto.ProtoType;
import tech.ydb.table.values.proto.ProtoValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static tech.ydb.table.YdbTable.ColumnFamily.Compression.COMPRESSION_LZ4;
import static tech.ydb.table.YdbTable.ColumnFamily.Compression.COMPRESSION_NONE;
import static tech.ydb.table.YdbTable.ColumnFamily.Compression.COMPRESSION_UNSPECIFIED;
import tech.ydb.table.settings.CreateSessionSettings;


/**
 * @author Sergey Polovko
 */
@ThreadSafe
public abstract class BaseSession implements Session {
    private final static Logger log = LoggerFactory.getLogger(Session.class);

    private final String id;
    private final TableRpc tableRpc;
    private final boolean keepQueryText;

    protected BaseSession(String id, TableRpc tableRpc, boolean keepQueryText) {
        this.id = id;
        this.tableRpc = tableRpc;
        this.keepQueryText = keepQueryText;
    }

    @Override
    public String getId() {
        return id;
    }

    public static CompletableFuture<Result<String>> createSessionId(TableRpc tableRpc, CreateSessionSettings settings) {
        YdbTable.CreateSessionRequest request = YdbTable.CreateSessionRequest.newBuilder()
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);

        return tableRpc.createSession(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(
                    response.expect("createSession()").getOperation(),
                    YdbTable.CreateSessionResult.class,
                    YdbTable.CreateSessionResult::getSessionId,
                    deadlineAfter);
            });
    }

    private static void applyPartitioningSettings(
            PartitioningSettings partitioningSettings,
            Consumer<YdbTable.PartitioningSettings> consumer) {
        if (partitioningSettings == null) {
            return;
        }

        YdbTable.PartitioningSettings.Builder builder = YdbTable.PartitioningSettings.newBuilder();
        if (partitioningSettings.getPartitioningByLoad() != null) {
            builder.setPartitioningByLoad(
                    partitioningSettings.getPartitioningByLoad()
                            ? CommonProtos.FeatureFlag.Status.ENABLED
                            : CommonProtos.FeatureFlag.Status.DISABLED
            );
        }
        if (partitioningSettings.getPartitioningBySize() != null) {
            builder.setPartitioningBySize(
                    partitioningSettings.getPartitioningBySize()
                            ? CommonProtos.FeatureFlag.Status.ENABLED
                            : CommonProtos.FeatureFlag.Status.DISABLED
            );
        }
        if (partitioningSettings.getPartitionSizeMb() != null) {
            builder.setPartitionSizeMb(partitioningSettings.getPartitionSizeMb());
        }
        if (partitioningSettings.getMinPartitionsCount() != null) {
            builder.setMinPartitionsCount(partitioningSettings.getMinPartitionsCount());
        }
        if (partitioningSettings.getMaxPartitionsCount() != null) {
            builder.setMaxPartitionsCount(partitioningSettings.getMaxPartitionsCount());
        }

        consumer.accept(builder.build());
    }

    @Override
    public CompletableFuture<Status> createTable(
        String path,
        TableDescription tableDescription,
        CreateTableSettings settings
    ) {
        YdbTable.CreateTableRequest.Builder request = YdbTable.CreateTableRequest.newBuilder()
            .setSessionId(id)
            .setPath(path)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .addAllPrimaryKey(tableDescription.getPrimaryKeys());

        applyPartitioningSettings(tableDescription.getPartitioningSettings(), request::setPartitioningSettings);

        for (TableColumn column : tableDescription.getColumns()) {
            YdbTable.ColumnMeta.Builder builder = YdbTable.ColumnMeta.newBuilder()
                    .setName(column.getName())
                    .setType(column.getType().toPb());
            if (column.getFamily() != null) {
                builder.setFamily(column.getFamily());
            }
            request.addColumns(builder.build());
        }

        for (TableIndex index : tableDescription.getIndexes()) {
            YdbTable.TableIndex.Builder b = request.addIndexesBuilder();
            b.setName(index.getName());
            b.addAllIndexColumns(index.getColumns());
            if (index.getType() == TableIndex.Type.GLOBAL) {
                b.setGlobalIndex(YdbTable.GlobalIndex.getDefaultInstance());
            }
        }


        for (ColumnFamily family : tableDescription.getColumnFamilies()) {
            YdbTable.ColumnFamily.Compression compression;
            switch (family.getCompression()) {
                case COMPRESSION_NONE:
                    compression = COMPRESSION_NONE;
                    break;
                case COMPRESSION_LZ4:
                    compression = COMPRESSION_LZ4;
                    break;
                default:
                    compression = COMPRESSION_UNSPECIFIED;
            }
            request.addColumnFamilies(
                YdbTable.ColumnFamily.newBuilder()
                    .setKeepInMemoryValue(family.isKeepInMemory() ?
                        tech.ydb.common.CommonProtos.FeatureFlag.Status.ENABLED.getNumber() :
                        tech.ydb.common.CommonProtos.FeatureFlag.Status.DISABLED.getNumber())
                    .setCompression(compression)
                    .setData(YdbTable.StoragePool.newBuilder().setMedia(family.getData().getMedia()))
                    .setName(family.getName())
                    .build());
        }

        if (settings.getPresetName() != null) {
            request.getProfileBuilder()
                .setPresetName(settings.getPresetName());
        }

        if (settings.getExecutionPolicy() != null) {
            request.getProfileBuilder()
                .getExecutionPolicyBuilder()
                .setPresetName(settings.getExecutionPolicy());
        }

        if (settings.getCompactionPolicy() != null) {
            request.getProfileBuilder()
                .getCompactionPolicyBuilder()
                .setPresetName(settings.getCompactionPolicy());
        }

        {
            PartitioningPolicy policy = settings.getPartitioningPolicy();
            if (policy != null) {
                YdbTable.PartitioningPolicy.Builder policyProto = request.getProfileBuilder()
                    .getPartitioningPolicyBuilder();
                if (policy.getPresetName() != null) {
                    policyProto.setPresetName(policy.getPresetName());
                }
                if (policy.getAutoPartitioning() != null) {
                    policyProto.setAutoPartitioning(toPb(policy.getAutoPartitioning()));
                }

                if (policy.getUniformPartitions() > 0) {
                    policyProto.setUniformPartitions(policy.getUniformPartitions());
                } else {
                    List<TupleValue> points = policy.getExplicitPartitioningPoints();
                    if (points != null) {
                        YdbTable.ExplicitPartitions.Builder b = policyProto.getExplicitPartitionsBuilder();
                        for (Value p : points) {
                            b.addSplitPoints(ProtoValue.toTypedValue(p));
                        }
                    }
                }
            }
        }

        {
            StoragePolicy policy = settings.getStoragePolicy();
            if (policy != null) {
                YdbTable.StoragePolicy.Builder policyProto = request.getProfileBuilder()
                    .getStoragePolicyBuilder();
                if (policy.getPresetName() != null) {
                    policyProto.setPresetName(policy.getPresetName());
                }
                if (policy.getSysLog() != null) {
                    policyProto.getSyslogBuilder().setMedia(policy.getSysLog());
                }
                if (policy.getLog() != null) {
                    policyProto.getLogBuilder().setMedia(policy.getLog());
                }
                if (policy.getData() != null) {
                    policyProto.getDataBuilder().setMedia(policy.getData());
                }
                if (policy.getExternal() != null) {
                    policyProto.getExternalBuilder().setMedia(policy.getExternal());
                }
            }
        }

        {
            ReplicationPolicy policy = settings.getReplicationPolicy();
            if (policy != null) {
                YdbTable.ReplicationPolicy.Builder replicationPolicyProto =
                    request.getProfileBuilder().getReplicationPolicyBuilder();
                if (policy.getPresetName() != null) {
                    replicationPolicyProto.setPresetName(policy.getPresetName());
                }
                replicationPolicyProto.setReplicasCount(policy.getReplicasCount());
                replicationPolicyProto.setCreatePerAvailabilityZone(policy.isCreatePerAvailabilityZone() ?
                    CommonProtos.FeatureFlag.Status.ENABLED : CommonProtos.FeatureFlag.Status.DISABLED);
                replicationPolicyProto.setAllowPromotion(policy.isAllowPromotion() ?
                    CommonProtos.FeatureFlag.Status.ENABLED : CommonProtos.FeatureFlag.Status.DISABLED);
            }
        }

        {
            TtlSettings ttlSettings = settings.getTtlSettings();
            if (ttlSettings != null) {
                YdbTable.DateTypeColumnModeSettings.Builder dateTypeColumnBuilder = request.getTtlSettingsBuilder().getDateTypeColumnBuilder();
                dateTypeColumnBuilder.setColumnName(ttlSettings.getDateTimeColumn());
                dateTypeColumnBuilder.setExpireAfterSeconds(ttlSettings.getExpireAfterSeconds());
            }
        }

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return tableRpc.createTable(request.build(), deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("createTable()").getOperation(), deadlineAfter);
            });
    }

    private static YdbTable.PartitioningPolicy.AutoPartitioningPolicy toPb(AutoPartitioningPolicy policy) {
        switch (policy) {
            case AUTO_SPLIT: return YdbTable.PartitioningPolicy.AutoPartitioningPolicy.AUTO_SPLIT;
            case AUTO_SPLIT_MERGE: return YdbTable.PartitioningPolicy.AutoPartitioningPolicy.AUTO_SPLIT_MERGE;
            case DISABLED: return YdbTable.PartitioningPolicy.AutoPartitioningPolicy.DISABLED;
        }
        throw new IllegalArgumentException("unknown AutoPartitioningPolicy: " + policy);
    }

    @Override
    public CompletableFuture<Status> dropTable(String path, DropTableSettings settings) {
        YdbTable.DropTableRequest request = YdbTable.DropTableRequest.newBuilder()
            .setSessionId(id)
            .setPath(path)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return tableRpc.dropTable(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("dropTable()").getOperation(), deadlineAfter);
            });
    }

    @Override
    public CompletableFuture<Status> alterTable(String path, AlterTableSettings settings) {
        YdbTable.AlterTableRequest.Builder builder = YdbTable.AlterTableRequest.newBuilder()
            .setSessionId(id)
            .setPath(path)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings));

        settings.forEachAddColumn((name, type) -> {
            builder.addAddColumns(YdbTable.ColumnMeta.newBuilder()
                .setName(name)
                .setType(type.toPb())
                .build());
        });

        settings.forEachDropColumn(builder::addDropColumns);

        TtlSettings ttlSettings = settings.getTtlSettings();
        if (ttlSettings != null) {
            YdbTable.DateTypeColumnModeSettings.Builder dateTypeColumnBuilder = builder.getSetTtlSettingsBuilder().getDateTypeColumnBuilder();
            dateTypeColumnBuilder.setColumnName(ttlSettings.getDateTimeColumn());
            dateTypeColumnBuilder.setExpireAfterSeconds(ttlSettings.getExpireAfterSeconds());
        }

        applyPartitioningSettings(settings.getPartitioningSettings(), builder::setAlterPartitioningSettings);

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return tableRpc.alterTable(builder.build(), deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("alterTable()").getOperation(), deadlineAfter);
            });
    }

    @Override
    public CompletableFuture<Status> copyTable(String src, String dst, CopyTableSettings settings) {
        YdbTable.CopyTableRequest request = YdbTable.CopyTableRequest.newBuilder()
            .setSessionId(id)
            .setSourcePath(src)
            .setDestinationPath(dst)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return tableRpc.copyTable(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("copyTable()").getOperation(), deadlineAfter);
            });
    }

    @Override
    public CompletableFuture<Result<TableDescription>> describeTable(String path, DescribeTableSettings settings) {
        YdbTable.DescribeTableRequest request = YdbTable.DescribeTableRequest.newBuilder()
                .setSessionId(id)
                .setPath(path)
                .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
                .setIncludeTableStats(settings.isIncludeTableStats())
                .setIncludeShardKeyBounds(settings.isIncludeShardKeyBounds())
                .setIncludePartitionStats(settings.isIncludePartitionStats())
                .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return tableRpc.describeTable(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(response.expect("describeTable()").getOperation(),
                    YdbTable.DescribeTableResult.class,
                    result -> BaseSession.mapDescribeTable(result, settings),
                    deadlineAfter);
            });
    }

    private static TableDescription mapDescribeTable(
            YdbTable.DescribeTableResult result,
            DescribeTableSettings describeTableSettings
    ) {
        TableDescription.Builder description = TableDescription.newBuilder();
        for (int i = 0; i < result.getColumnsCount(); i++) {
            YdbTable.ColumnMeta column = result.getColumns(i);
            description.addNonnullColumn(column.getName(), ProtoType.fromPb(column.getType()), column.getFamily());
        }
        description.setPrimaryKeys(result.getPrimaryKeyList());
        for (int i = 0; i < result.getIndexesCount(); i++) {
            YdbTable.TableIndexDescription index = result.getIndexes(i);
            description.addGlobalIndex(index.getName(), index.getIndexColumnsList(), index.getDataColumnsList());
        }
        YdbTable.TableStats tableStats = result.getTableStats();
        if (describeTableSettings.isIncludeTableStats() && tableStats != null) {
            Timestamp creationTime = tableStats.getCreationTime();
            Instant createdAt = creationTime == null ? null : Instant.ofEpochSecond(creationTime.getSeconds(), creationTime.getNanos());
            Timestamp modificationTime = tableStats.getCreationTime();
            Instant modifiedAt = modificationTime == null ? null : Instant.ofEpochSecond(modificationTime.getSeconds(), modificationTime.getNanos());
            TableDescription.TableStats stats = new TableDescription.TableStats(
                    createdAt, modifiedAt, tableStats.getRowsEstimate(), tableStats.getStoreSize());
            description.setTableStats(stats);

            List<YdbTable.PartitionStats> partitionStats = tableStats.getPartitionStatsList();
            if (describeTableSettings.isIncludePartitionStats() && partitionStats != null) {
                for (YdbTable.PartitionStats stat: partitionStats) {
                    description.addPartitionStat(stat.getRowsEstimate(), stat.getStoreSize());
                }
            }
        }
        YdbTable.PartitioningSettings partitioningSettings = result.getPartitioningSettings();
        if (partitioningSettings != null) {
            PartitioningSettings settings = new PartitioningSettings();
            settings.setPartitionSize(partitioningSettings.getPartitionSizeMb());
            settings.setMinPartitionsCount(partitioningSettings.getMinPartitionsCount());
            settings.setMaxPartitionsCount(partitioningSettings.getMaxPartitionsCount());
            settings.setPartitioningByLoad(partitioningSettings.getPartitioningByLoad() == CommonProtos.FeatureFlag.Status.ENABLED);
            settings.setPartitioningBySize(partitioningSettings.getPartitioningBySize() == CommonProtos.FeatureFlag.Status.ENABLED);
            description.setPartitioningSettings(settings);
        }

        List<YdbTable.ColumnFamily> columnFamiliesList = result.getColumnFamiliesList();
        if (columnFamiliesList != null) {
            for (YdbTable.ColumnFamily family : columnFamiliesList) {
                ColumnFamily.Compression compression;
                switch (family.getCompression()) {
                    case COMPRESSION_LZ4:
                        compression = ColumnFamily.Compression.COMPRESSION_LZ4;
                        break;
                    default:
                        compression = ColumnFamily.Compression.COMPRESSION_NONE;
                }
                description.addColumnFamily(
                        new ColumnFamily(family.getName(),
                                new StoragePool(family.getData().getMedia()),
                                compression,
                                family.getKeepInMemory().equals(tech.ydb.common.CommonProtos.FeatureFlag.Status.ENABLED))
                );
            }
        }
        if (describeTableSettings.isIncludeShardKeyBounds()) {
            List<ValueProtos.TypedValue> shardKeyBoundsList = result.getShardKeyBoundsList();
            if (shardKeyBoundsList != null) {
                Optional<Value> leftValue = Optional.empty();
                for (ValueProtos.TypedValue typedValue : shardKeyBoundsList) {
                    Optional<KeyBound> fromBound = leftValue.map(KeyBound::inclusive);
                    Value value = ProtoValue.fromPb(
                            ProtoType.fromPb(typedValue.getType()),
                            typedValue.getValue()
                    );
                    Optional<KeyBound> toBound = Optional.of(KeyBound.exclusive(value));
                    description.addKeyRange(new KeyRange(fromBound, toBound));
                    leftValue = Optional.of(value);
                }
                description.addKeyRange(
                        new KeyRange(leftValue.map(KeyBound::inclusive), Optional.empty())
                );
            }
        }

        return description.build();
    }

    private static YdbTable.TransactionSettings txSettings(Transaction.Mode transactionMode) {
        YdbTable.TransactionSettings.Builder settings = YdbTable.TransactionSettings.newBuilder();
        if (transactionMode != null) {
            switch (transactionMode) {
                case SERIALIZABLE_READ_WRITE:
                    settings.setSerializableReadWrite(YdbTable.SerializableModeSettings.getDefaultInstance());
                    break;
                case ONLINE_READ_ONLY:
                    settings.setOnlineReadOnly(YdbTable.OnlineModeSettings.getDefaultInstance());
                    break;
                case STALE_READ_ONLY:
                    settings.setStaleReadOnly(YdbTable.StaleModeSettings.getDefaultInstance());
                    break;
                default:
                    break;
            }
        }
        return settings.build();
    }

    @Override
    public CompletableFuture<Result<DataQueryResult>> executeDataQuery(
        String query, TxControl txControl, Params params, ExecuteDataQuerySettings settings)
    {
        YdbTable.ExecuteDataQueryRequest.Builder request = YdbTable.ExecuteDataQueryRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setTxControl(txControl.toPb())
            .setQuery(YdbTable.Query.newBuilder().setYqlText(query))
            .setCollectStats(settings.collectStats())
            .putAllParameters(params.toPb());

        final boolean keepInServerQueryCache = settings.isKeepInQueryCache();
        if (keepInServerQueryCache) {
            request.getQueryCachePolicyBuilder()
                .setKeepInCache(true);
        }

        String msg = "query";
        if (log.isDebugEnabled() && keepQueryText) {
            StringBuilder sb = new StringBuilder(query.replaceAll("\\s", " "));
            if (!params.isEmpty()) {
                sb.append(" [");
                boolean one = true;
                for (Map.Entry<String, Value<?>> entry : params.values().entrySet()) {
                    if (!one) {
                        sb.append(", ");
                    }
                    sb.append(entry.getKey());
                    sb.append("=");
                    sb.append(entry.getValue());
                    one = false;
                }
                sb.append("]");
            }
            msg = sb.toString();
        }

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResultWithLog(msg, tableRpc.executeDataQuery(request.build(), deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                Operation operation = response.expect("executeDataQuery()").getOperation();
                DataQueryResult.CostInfo costInfo = operation.hasCostInfo()
                        ? new DataQueryResult.CostInfo(operation.getCostInfo())
                        : null;
                return tableRpc.getOperationTray().waitResult(
                    operation,
                    YdbTable.ExecuteQueryResult.class,
                    result -> new DataQueryResult(
                            result.getTxMeta().getId(),
                            result.getResultSetsList(),
                            costInfo
                    ),
                    deadlineAfter);
            }));
    }

    CompletableFuture<Result<DataQueryResult>> executePreparedDataQuery(
        String queryId, @Nullable String queryText, TxControl txControl, Params params, ExecuteDataQuerySettings settings)
    {
        YdbTable.ExecuteDataQueryRequest.Builder request = YdbTable.ExecuteDataQueryRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setTxControl(txControl.toPb())
            .setCollectStats(settings.collectStats());

        request.getQueryBuilder().setId(queryId);
        request.putAllParameters(params.toPb());

        final boolean keepInServerQueryCache = settings.isKeepInQueryCache();
        if (keepInServerQueryCache) {
            request.getQueryCachePolicyBuilder()
                .setKeepInCache(true);
        }

        String msg = "prepared query";
        if (log.isDebugEnabled() && keepQueryText) {
            StringBuilder sb = new StringBuilder("prepared,");
            if (queryText != null) {
                sb.append(queryText.replaceAll("\\s", " "));
            }
            if (!params.isEmpty()) {
                sb.append(" [");
                boolean one = true;
                for (Map.Entry<String, Value<?>> entry : params.values().entrySet()) {
                    if (!one) {
                        sb.append(", ");
                    }
                    sb.append(entry.getKey());
                    sb.append("=");
                    sb.append(entry.getValue());
                    one = false;
                }
                sb.append("]");
            }
            msg = sb.toString();
        }

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResultWithLog(msg, tableRpc.executeDataQuery(request.build(), deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                Operation operation = response.expect("executeDataQuery()").getOperation();
                DataQueryResult.CostInfo costInfo = operation.hasCostInfo()
                        ? new DataQueryResult.CostInfo(operation.getCostInfo())
                        : null;

                return tableRpc.getOperationTray().waitResult(
                    operation,
                    YdbTable.ExecuteQueryResult.class,
                    result -> new DataQueryResult(
                            result.getTxMeta().getId(),
                            result.getResultSetsList(),
                            costInfo
                    ),
                    deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Result<DataQuery>> prepareDataQuery(String query, PrepareDataQuerySettings settings) {
        YdbTable.PrepareDataQueryRequest.Builder request = YdbTable.PrepareDataQueryRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setYqlText(query);

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResult(tableRpc.prepareDataQuery(request.build(), deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(
                    response.expect("prepareDataQuery()").getOperation(),
                    YdbTable.PrepareQueryResult.class,
                    result -> {
                        String queryId = result.getQueryId();
                        Map<String, ValueProtos.Type> types = result.getParametersTypesMap();
                        DataQueryImpl dataQuery = new DataQueryImpl(this, queryId, query, keepQueryText, types);
                        return dataQuery;
                    },
                    deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Status> executeSchemeQuery(String query, ExecuteSchemeQuerySettings settings) {
        YdbTable.ExecuteSchemeQueryRequest request = YdbTable.ExecuteSchemeQueryRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setYqlText(query)
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptStatus(tableRpc.executeSchemeQuery(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("executeSchemaQuery()").getOperation(), deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Result<ExplainDataQueryResult>> explainDataQuery(String query, ExplainDataQuerySettings settings) {
        YdbTable.ExplainDataQueryRequest request = YdbTable.ExplainDataQueryRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setYqlText(query)
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResult(tableRpc.explainDataQuery(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(
                    response.expect("explainDataQuery()").getOperation(),
                    YdbTable.ExplainQueryResult.class,
                    result -> new ExplainDataQueryResult(result.getQueryAst(), result.getQueryPlan()),
                    deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Result<Transaction>> beginTransaction(Transaction.Mode transactionMode, BeginTxSettings settings) {
        YdbTable.BeginTransactionRequest request = YdbTable.BeginTransactionRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setTxSettings(txSettings(transactionMode))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResultWithLog("begin transaction", tableRpc.beginTransaction(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(
                    response.expect("beginTransaction()").getOperation(),
                    YdbTable.BeginTransactionResult.class,
                    result -> new TransactionImpl(this, result.getTxMeta().getId()),
                    deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Status> readTable(String tablePath, ReadTableSettings settings, Consumer<ResultSetReader> fn) {
        ReadTableRequest.Builder request = ReadTableRequest.newBuilder()
            .setSessionId(id)
            .setPath(tablePath)
            .setOrdered(settings.isOrdered())
            .setRowLimit(settings.getRowLimit());

        Value fromKey = settings.getFromKey();
        if (fromKey != null) {
            YdbTable.KeyRange.Builder range = request.getKeyRangeBuilder();
            if (settings.isFromInclusive()) {
                range.setGreaterOrEqual(ProtoValue.toTypedValue(fromKey));
            } else {
                range.setGreater(ProtoValue.toTypedValue(fromKey));
            }
        }

        Value toKey = settings.getToKey();
        if (toKey != null) {
            YdbTable.KeyRange.Builder range = request.getKeyRangeBuilder();
            if (settings.isToInclusive()) {
                range.setLessOrEqual(ProtoValue.toTypedValue(toKey));
            } else {
                range.setLess(ProtoValue.toTypedValue(toKey));
            }
        }

        if (!settings.getColumns().isEmpty()) {
            request.addAllColumns(settings.getColumns());
        }

        final long deadlineAfter = settings.getDeadlineAfter();
        CompletableFuture<Status> promise = new CompletableFuture<>();
        StreamControl control = tableRpc.streamReadTable(request.build(), new StreamObserver<ReadTableResponse>() {
            @Override
            public void onNext(ReadTableResponse response) {
                StatusIds.StatusCode statusCode = response.getStatus();
                if (statusCode == StatusIds.StatusCode.SUCCESS) {
                    try {
                        fn.accept(ProtoValueReaders.forResultSet(response.getResult().getResultSet()));
                    } catch (Throwable t) {
                        promise.completeExceptionally(t);
                        throw new IllegalStateException(t);
                    }
                } else {
                    Issue[] issues = Issue.fromPb(response.getIssuesList());
                    StatusCode code = StatusCode.fromProto(statusCode);
                    promise.complete(Status.of(code, issues));
                }
            }

            @Override
            public void onError(Status status) {
                assert !status.isSuccess();
                promise.complete(status);
            }

            @Override
            public void onCompleted() {
                promise.complete(Status.SUCCESS);
            }
        }, deadlineAfter);
        return promise.whenComplete((status, ex) -> {
            if (ex instanceof CancellationException) {
                control.cancel();
            }
        });
    }

    @Override
    public CompletableFuture<Status> executeScanQuery(String query, Params params, ExecuteScanQuerySettings settings, Consumer<ResultSetReader> fn)
    {
        YdbTable.ExecuteScanQueryRequest request = YdbTable.ExecuteScanQueryRequest.newBuilder()
            .setQuery(YdbTable.Query.newBuilder().setYqlText(query))
            .setMode(settings.getMode())
            .putAllParameters(params.toPb())
            .setCollectStats(settings.getCollectStats())
            .build();

        CompletableFuture<Status> promise = new CompletableFuture<>();
        final long deadlineAfter = settings.getDeadlineAfter();
        StreamControl control = tableRpc.streamExecuteScanQuery(request, new StreamObserver<YdbTable.ExecuteScanQueryPartialResponse>() {
            @Override
            public void onNext(YdbTable.ExecuteScanQueryPartialResponse response) {
                StatusIds.StatusCode statusCode = response.getStatus();
                if (statusCode == StatusIds.StatusCode.SUCCESS) {
                    try {
                        fn.accept(ProtoValueReaders.forResultSet(response.getResult().getResultSet()));
                    } catch (Throwable t) {
                        promise.completeExceptionally(t);
                        throw new IllegalStateException(t);
                    }
                } else {
                    Issue[] issues = Issue.fromPb(response.getIssuesList());
                    StatusCode code = StatusCode.fromProto(statusCode);
                    promise.complete(Status.of(code, issues));
                }
            }

            @Override
            public void onError(Status status) {
                assert !status.isSuccess();
                promise.complete(status);
            }

            @Override
            public void onCompleted() {
                promise.complete(Status.SUCCESS);
            }
        }, deadlineAfter);
        return promise.whenComplete((status, ex) -> {
            if (ex instanceof CancellationException) {
                control.cancel();
            }
        });
    }

    @Override
    public CompletableFuture<Status> commitTransaction(String txId, CommitTxSettings settings) {
        YdbTable.CommitTransactionRequest request = YdbTable.CommitTransactionRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setTxId(txId)
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptStatusWithLog("commit transaction", tableRpc.commitTransaction(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray()
                    .waitStatus(response.expect("commitTransaction()").getOperation(), deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Status> rollbackTransaction(String txId, RollbackTxSettings settings) {
        YdbTable.RollbackTransactionRequest request = YdbTable.RollbackTransactionRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .setTxId(txId)
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptStatusWithLog("rollback transaction", tableRpc.rollbackTransaction(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray()
                    .waitStatus(response.expect("rollbackTransaction()").getOperation(), deadlineAfter);
            }));
    }

    @Override
    public CompletableFuture<Result<State>> keepAlive(KeepAliveSessionSettings settings) {
        YdbTable.KeepAliveRequest request = YdbTable.KeepAliveRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptResult(tableRpc.keepAlive(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.cast());
                }
                return tableRpc.getOperationTray().waitResult(response.expect("keepAlive()").getOperation(),
                    YdbTable.KeepAliveResult.class,
                    BaseSession::mapSessionStatus,
                    deadlineAfter
                );
            }));
    }

    @Override
    public CompletableFuture<Status> executeBulkUpsert(String tablePath, ListValue rows, BulkUpsertSettings settings) {
        ValueProtos.TypedValue typedRows = ValueProtos.TypedValue.newBuilder()
            .setType(rows.getType().toPb())
            .setValue(rows.toPb())
            .build();

        YdbTable.BulkUpsertRequest request = YdbTable.BulkUpsertRequest.newBuilder()
            .setTable(tablePath)
            .setRows(typedRows)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);

        return interceptStatus(tableRpc.bulkUpsert(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("bulkUpsert()").getOperation(), deadlineAfter);
            }));
    }

    private static State mapSessionStatus(YdbTable.KeepAliveResult result) {
        switch (result.getSessionStatus()) {
            case UNRECOGNIZED:
            case SESSION_STATUS_UNSPECIFIED: return State.UNSPECIFIED;
            case SESSION_STATUS_BUSY: return State.BUSY;
            case SESSION_STATUS_READY: return State.READY;
        }
        throw new IllegalStateException("unknown session status: " + result.getSessionStatus());
    }

    public CompletableFuture<Status> delete(DeleteSessionSettings settings) {
        YdbTable.DeleteSessionRequest request = YdbTable.DeleteSessionRequest.newBuilder()
            .setSessionId(id)
            .setOperationParams(OperationParamUtils.fromRequestSettings(settings))
            .build();

        final long deadlineAfter = RequestSettingsUtils.calculateDeadlineAfter(settings);
        return interceptStatus(tableRpc.deleteSession(request, deadlineAfter)
            .thenCompose(response -> {
                if (!response.isSuccess()) {
                    return CompletableFuture.completedFuture(response.toStatus());
                }
                return tableRpc.getOperationTray().waitStatus(response.expect("deleteSession()").getOperation(), deadlineAfter);
            })
        );
    }

    private <T> CompletableFuture<Result<T>> interceptResultWithLog(String msg, CompletableFuture<Result<T>> future) {
        final long start = Instant.now().toEpochMilli();
        return future.whenComplete((r, t) -> {
            long ms = Instant.now().toEpochMilli() - start;
            log.debug("Session[{}] {} => {}, took {} ms", hashCode(), msg, r.getCode(), ms);
            updateSessionState(t, r.getCode());
        });
    }

    private <T> CompletableFuture<Result<T>> interceptResult(CompletableFuture<Result<T>> future) {
        return future.whenComplete((r, t) -> {
            updateSessionState(t, r.getCode());
        });
    }

    private CompletableFuture<Status> interceptStatus(CompletableFuture<Status> future) {
        return future.whenComplete((r, t) -> {
            updateSessionState(t, r.getCode());
        });
    }

    private CompletableFuture<Status> interceptStatusWithLog(String msg, CompletableFuture<Status> future) {
        final long start = Instant.now().toEpochMilli();
        return future.whenComplete((r, t) -> {
            long ms = Instant.now().toEpochMilli() - start;
            log.debug("Session[{}] {} => {}, took {} ms", hashCode(), msg, r.getCode(), ms);
            updateSessionState(t, r.getCode());
        });
    }
    
    protected abstract void updateSessionState(Throwable th, StatusCode code);

    @Override
    public String toString() {
        return "Session{" + id + "}";
    }
}