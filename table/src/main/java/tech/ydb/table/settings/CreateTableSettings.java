package tech.ydb.table.settings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ydb.table.description.TableDescription;


/**
 * @author Sergey Polovko
 */
public class CreateTableSettings extends RequestSettings<CreateTableSettings> {

    @Nullable
    private String presetName;
    @Nullable
    private String executionPolicy;
    @Nullable
    private String compactionPolicy;
    @Nullable
    private PartitioningPolicy partitioningPolicy;
    @Nullable
    private StoragePolicy storagePolicy;
    @Nullable
    private ReplicationPolicy replicationPolicy;
    @Nullable
    @SuppressWarnings("deprecation")
    private TtlSettings ttlSettings;

    @Nullable
    public String getPresetName() {
        return presetName;
    }

    public CreateTableSettings setPresetName(@Nonnull String presetName) {
        this.presetName = presetName;
        return this;
    }

    @Nullable
    public String getExecutionPolicy() {
        return executionPolicy;
    }

    public CreateTableSettings setExecutionPolicy(@Nonnull String executionPolicy) {
        this.executionPolicy = executionPolicy;
        return this;
    }

    @Nullable
    public String getCompactionPolicy() {
        return compactionPolicy;
    }

    public CreateTableSettings setCompactionPolicy(@Nonnull String compactionPolicy) {
        this.compactionPolicy = compactionPolicy;
        return this;
    }

    @Nullable
    public PartitioningPolicy getPartitioningPolicy() {
        return partitioningPolicy;
    }

    public CreateTableSettings setPartitioningPolicy(@Nonnull PartitioningPolicy partitioningPolicy) {
        this.partitioningPolicy = partitioningPolicy;
        return this;
    }

    @Nullable
    public StoragePolicy getStoragePolicy() {
        return storagePolicy;
    }

    public CreateTableSettings setStoragePolicy(@Nonnull StoragePolicy storagePolicy) {
        this.storagePolicy = storagePolicy;
        return this;
    }

    @Nullable
    public ReplicationPolicy getReplicationPolicy() {
        return replicationPolicy;
    }

    public CreateTableSettings setReplicationPolicy(@Nullable ReplicationPolicy replicationPolicy) {
        this.replicationPolicy = replicationPolicy;
        return this;
    }

    /**
     * @return TTL configuration
     * @deprecated use {@link TableDescription#getTableTtl() } instead
     */
    @Nullable
    @Deprecated
    public TtlSettings getTtlSettings() {
        return ttlSettings;
    }

    /**
     * @param ttlSettings TTL configuration
     * @deprecated use {@link TableDescription.Builder#setTtlSettings(tech.ydb.table.description.TableTtl)} instead
     */
    @Deprecated
    public void setTtlSettings(@Nullable TtlSettings ttlSettings) {
        this.ttlSettings = ttlSettings;
    }
}
