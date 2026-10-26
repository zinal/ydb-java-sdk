package tech.ydb.cdc;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.table.values.Type;

/**
 * Column of a table schema snapshot used to decode CDC records.
 */
public final class CdcColumn {
    private final String name;
    @Nullable
    private final Type type;
    private final String typeName;
    @Nullable
    private final String family;

    public CdcColumn(String name, @Nullable Type type, String typeName, @Nullable String family) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = type;
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.family = family;
    }

    public String getName() {
        return name;
    }

    /**
     * @return YDB type, or {@code null} when the snapshot name is not representable by this SDK
     */
    @Nullable
    public Type getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    @Nullable
    public String getFamily() {
        return family;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CdcColumn column = (CdcColumn) other;
        return name.equals(column.name)
                && Objects.equals(type, column.type)
                && typeName.equals(column.typeName)
                && Objects.equals(family, column.family);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, typeName, family);
    }

    @Override
    public String toString() {
        return name + ' ' + typeName;
    }
}
