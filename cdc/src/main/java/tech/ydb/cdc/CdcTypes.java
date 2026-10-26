package tech.ydb.cdc;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PgType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Type;

/**
 * YDB type names used in CDC schema snapshots, and fixed cell sizes of the binary format.
 */
final class CdcTypes {
    private CdcTypes() {
    }

    static String format(Type type) {
        if (type instanceof OptionalType) {
            return "Optional<" + format(((OptionalType) type).getItemType()) + ">";
        }
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return "Decimal(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
        }
        if (type instanceof PgType) {
            return "Pg(" + ((PgType) type).getOid() + ")";
        }
        if (type instanceof PrimitiveType) {
            PrimitiveType primitive = (PrimitiveType) type;
            if (primitive == PrimitiveType.Text) {
                return "Utf8";
            }
            if (primitive == PrimitiveType.Bytes) {
                return "String";
            }
            return primitive.name();
        }
        return String.valueOf(type);
    }

    static Type parse(String typeName) {
        String name = typeName.trim();
        if (name.endsWith("?")) {
            Type inner = parse(name.substring(0, name.length() - 1));
            return inner == null ? null : OptionalType.of(inner);
        }
        if (name.startsWith("Optional<") && name.endsWith(">")) {
            Type inner = parse(name.substring("Optional<".length(), name.length() - 1));
            return inner == null ? null : OptionalType.of(inner);
        }
        if (name.startsWith("Decimal(") && name.endsWith(")")) {
            String body = name.substring("Decimal(".length(), name.length() - 1);
            int comma = body.indexOf(',');
            if (comma < 0) {
                return null;
            }
            int precision = Integer.parseInt(body.substring(0, comma).trim());
            int scale = Integer.parseInt(body.substring(comma + 1).trim());
            return DecimalType.of(precision, scale);
        }
        if (name.startsWith("Pg(") && name.endsWith(")")) {
            int oid = Integer.parseInt(name.substring(3, name.length() - 1).trim());
            return PgType.of(oid);
        }
        return primitive(name);
    }

    /**
     * @return number of bytes occupied by a non-null cell, or {@code 0} when the cell is length-prefixed
     */
    static int fixedSize(Type type) {
        Type inner = type instanceof OptionalType ? ((OptionalType) type).getItemType() : type;
        if (inner instanceof DecimalType) {
            return 16;
        }
        if (!(inner instanceof PrimitiveType)) {
            return 0;
        }
        switch ((PrimitiveType) inner) {
            case Bool:
            case Int8:
            case Uint8:
                return 1;
            case Int16:
            case Uint16:
            case Date:
                return 2;
            case Int32:
            case Uint32:
            case Float:
            case Date32:
            case Datetime:
                return 4;
            case Int64:
            case Uint64:
            case Double:
            case Timestamp:
            case Interval:
            case Datetime64:
            case Timestamp64:
            case Interval64:
                return 8;
            case Uuid:
                return 16;
            default:
                return 0;
        }
    }

    private static PrimitiveType primitive(String name) {
        if ("Utf8".equals(name) || "Text".equals(name)) {
            return PrimitiveType.Text;
        }
        if ("String".equals(name) || "Bytes".equals(name)) {
            return PrimitiveType.Bytes;
        }
        try {
            return PrimitiveType.valueOf(name);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
