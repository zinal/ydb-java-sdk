package tech.ydb.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Secondary index stored in a CDC schema snapshot.
 */
public final class CdcIndex {
    private final String name;
    private final String typeName;
    private final List<String> columns;
    private final List<String> dataColumns;

    public CdcIndex(String name, String typeName, List<String> columns, List<String> dataColumns) {
        this.name = Objects.requireNonNull(name, "name");
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.columns = Collections.unmodifiableList(new ArrayList<String>(columns));
        this.dataColumns = Collections.unmodifiableList(new ArrayList<String>(dataColumns));
    }

    public String getName() {
        return name;
    }

    public String getTypeName() {
        return typeName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getDataColumns() {
        return dataColumns;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CdcIndex index = (CdcIndex) other;
        return name.equals(index.name)
                && typeName.equals(index.typeName)
                && columns.equals(index.columns)
                && dataColumns.equals(index.dataColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, typeName, columns, dataColumns);
    }
}
