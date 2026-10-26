package tech.ydb.cdc;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ydb.table.values.Value;

/**
 * One named cell of a decoded CDC record.
 * {@code value} is {@code null} when a non-optional column is NULL.
 * An optional NULL is an empty {@link tech.ydb.table.values.OptionalValue}.
 */
public final class CdcCell {
    private final String name;
    @Nullable
    private final Value<?> value;

    public CdcCell(String name, @Nullable Value<?> value) {
        this.name = Objects.requireNonNull(name, "name");
        this.value = value;
    }

    public String getName() {
        return name;
    }

    @Nullable
    public Value<?> getValue() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CdcCell cell = (CdcCell) other;
        return name.equals(cell.name) && Objects.equals(value, cell.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return name + '=' + value;
    }
}
