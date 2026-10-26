package tech.ydb.cdc;

import java.util.LinkedHashMap;
import java.util.Map;

final class MemoryRows implements SchemaRows {
    private final Map<String, CdcTableSchema> rows = new LinkedHashMap<String, CdcTableSchema>();

    @Override
    public CdcTableSchema read(String tablePath) {
        return rows.get(tablePath);
    }

    @Override
    public void write(String tablePath, CdcTableSchema schema) {
        rows.put(tablePath, schema);
    }
}
