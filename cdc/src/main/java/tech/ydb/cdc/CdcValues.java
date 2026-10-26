package tech.ydb.cdc;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Base64;

import javax.annotation.Nullable;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import tech.ydb.table.values.DecimalType;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.OptionalValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;
import tech.ydb.table.values.proto.ProtoValue;

/**
 * Converts JSON tokens and binary cells into YDB values.
 */
final class CdcValues {
    private CdcValues() {
    }

    static long uint64(JsonElement element) {
        JsonPrimitive primitive = element.getAsJsonPrimitive();
        if (primitive.isString()) {
            return Long.parseUnsignedLong(primitive.getAsString());
        }
        return primitive.getAsBigInteger().longValue();
    }

    @Nullable
    static Value<?> fromJson(@Nullable JsonElement element, Type type, int offset) {
        if (element == null || element.isJsonNull()) {
            return nullValue(type);
        }
        if (type instanceof OptionalType) {
            OptionalType optional = (OptionalType) type;
            Value<?> inner = fromJson(element, optional.getItemType(), offset);
            if (inner == null) {
                return optional.emptyValue();
            }
            return OptionalValue.of(inner);
        }
        if (type == null) {
            throw new CdcParseException("CDC column type is not supported by the SDK", offset);
        }
        try {
            return presentJson(element, type, offset);
        } catch (RuntimeException ex) {
            throw new CdcParseException("cannot read CDC JSON value of type " + type + ": " + element, offset, ex);
        }
    }

    @Nullable
    static Value<?> fromCell(ByteBuffer buffer, Type type, boolean isNull, int offset) {
        if (isNull) {
            return nullValue(type);
        }
        if (type instanceof OptionalType) {
            OptionalType optional = (OptionalType) type;
            return OptionalValue.of(presentCell(buffer, optional.getItemType(), offset));
        }
        if (type == null) {
            throw new CdcParseException("CDC column type is not supported by the SDK", offset);
        }
        return presentCell(buffer, type, offset);
    }

    @Nullable
    private static Value<?> nullValue(Type type) {
        if (type instanceof OptionalType) {
            return ((OptionalType) type).emptyValue();
        }
        return null;
    }

    private static Value<?> presentJson(JsonElement element, Type type, int offset) {
        if (type instanceof DecimalType) {
            return ((DecimalType) type).newValue(element.getAsString());
        }
        if (!(type instanceof PrimitiveType)) {
            throw new CdcParseException("unsupported CDC type " + type, offset);
        }
        PrimitiveType primitive = (PrimitiveType) type;
        switch (primitive) {
            case Bool:
                return PrimitiveValue.newBool(element.getAsBoolean());
            case Int8:
                return PrimitiveValue.newInt8(element.getAsByte());
            case Uint8:
                return PrimitiveValue.newUint8(element.getAsInt());
            case Int16:
                return PrimitiveValue.newInt16(element.getAsShort());
            case Uint16:
                return PrimitiveValue.newUint16(element.getAsInt());
            case Int32:
                return PrimitiveValue.newInt32(element.getAsInt());
            case Uint32:
                return PrimitiveValue.newUint32(element.getAsLong());
            case Int64:
                return PrimitiveValue.newInt64(signed64(element));
            case Uint64:
                return PrimitiveValue.newUint64(uint64(element));
            case Float:
                return PrimitiveValue.newFloat(floatValue(element));
            case Double:
                return PrimitiveValue.newDouble(doubleValue(element));
            case Bytes:
                return PrimitiveValue.newBytes(Base64.getDecoder().decode(element.getAsString()));
            case Text:
                return PrimitiveValue.newText(element.getAsString());
            case Json:
                return PrimitiveValue.newJson(element.toString());
            case JsonDocument:
                return PrimitiveValue.newJsonDocument(element.toString());
            case Yson:
                return PrimitiveValue.newYson(element.toString().getBytes(StandardCharsets.UTF_8));
            case Uuid:
                return PrimitiveValue.newUuid(element.getAsString());
            case Date:
                return date(element);
            case Datetime:
                return datetime(element);
            case Timestamp:
                return timestamp(element);
            case Interval:
                return PrimitiveValue.newInterval(signed64(element));
            case Date32:
                return PrimitiveValue.newDate32(element.getAsInt());
            case Datetime64:
                return PrimitiveValue.newDatetime64(signed64(element));
            case Timestamp64:
                return PrimitiveValue.newTimestamp64(signed64(element));
            case Interval64:
                return PrimitiveValue.newInterval64(signed64(element));
            case TzDate:
                return PrimitiveValue.newTzDate(ProtoValue.toTzDate(element.getAsString()));
            case TzDatetime:
                return PrimitiveValue.newTzDatetime(ProtoValue.toTzDatetime(element.getAsString()));
            case TzTimestamp:
                return PrimitiveValue.newTzTimestamp(ProtoValue.toTzTimestamp(element.getAsString()));
            case DyNumber:
            default:
                throw new CdcParseException("unsupported CDC type " + primitive, offset);
        }
    }

    private static Value<?> presentCell(ByteBuffer buffer, Type type, int offset) {
        int fixed = CdcTypes.fixedSize(type);
        if (fixed > 0) {
            if (buffer.remaining() < fixed) {
                throw new CdcParseException("truncated binary CDC cell", offset);
            }
            return fixedCell(buffer, type, fixed, offset);
        }
        if (buffer.remaining() < Integer.BYTES) {
            throw new CdcParseException("truncated binary CDC cell", offset);
        }
        long size = Integer.toUnsignedLong(buffer.getInt());
        if (size > buffer.remaining()) {
            throw new CdcParseException("truncated binary CDC cell", offset);
        }
        byte[] raw = new byte[(int) size];
        buffer.get(raw);
        return variableCell(raw, type, offset);
    }

    private static Value<?> fixedCell(ByteBuffer buffer, Type type, int fixed, int offset) {
        if (type instanceof DecimalType) {
            long low = buffer.getLong();
            long high = buffer.getLong();
            return ((DecimalType) type).newValue(high, low);
        }
        PrimitiveType primitive = (PrimitiveType) type;
        switch (primitive) {
            case Bool:
                int flag = buffer.get() & 0xff;
                if (flag > 1) {
                    throw new CdcParseException("binary CDC bool is not 0 or 1", offset);
                }
                return PrimitiveValue.newBool(flag == 1);
            case Int8:
                return PrimitiveValue.newInt8(buffer.get());
            case Uint8:
                return PrimitiveValue.newUint8(buffer.get() & 0xff);
            case Int16:
                return PrimitiveValue.newInt16(buffer.getShort());
            case Uint16:
                return PrimitiveValue.newUint16(buffer.getShort() & 0xffff);
            case Int32:
                return PrimitiveValue.newInt32(buffer.getInt());
            case Uint32:
                return PrimitiveValue.newUint32(Integer.toUnsignedLong(buffer.getInt()));
            case Int64:
                return PrimitiveValue.newInt64(buffer.getLong());
            case Uint64:
                return PrimitiveValue.newUint64(buffer.getLong());
            case Float:
                return PrimitiveValue.newFloat(buffer.getFloat());
            case Double:
                return PrimitiveValue.newDouble(buffer.getDouble());
            case Date:
                return PrimitiveValue.newDate(buffer.getShort() & 0xffff);
            case Datetime:
                return PrimitiveValue.newDatetime(Integer.toUnsignedLong(buffer.getInt()));
            case Timestamp:
                return PrimitiveValue.newTimestamp(buffer.getLong());
            case Interval:
                return PrimitiveValue.newInterval(buffer.getLong());
            case Date32:
                return PrimitiveValue.newDate32(buffer.getInt());
            case Datetime64:
                return PrimitiveValue.newDatetime64(buffer.getLong());
            case Timestamp64:
                return PrimitiveValue.newTimestamp64(buffer.getLong());
            case Interval64:
                return PrimitiveValue.newInterval64(buffer.getLong());
            case Uuid:
                long low = buffer.getLong();
                long high = buffer.getLong();
                return ProtoValue.newUuid(high, low);
            default:
                throw new CdcParseException("fixed binary CDC cell has no reader for " + primitive
                        + " (" + fixed + " bytes)", offset);
        }
    }

    private static Value<?> variableCell(byte[] raw, Type type, int offset) {
        if (!(type instanceof PrimitiveType)) {
            throw new CdcParseException("unsupported binary CDC type " + type, offset);
        }
        PrimitiveType primitive = (PrimitiveType) type;
        switch (primitive) {
            case Bytes:
                return PrimitiveValue.newBytes(raw);
            case Text:
                return PrimitiveValue.newText(utf8(raw, offset));
            case Json:
                return PrimitiveValue.newJson(utf8(raw, offset));
            case JsonDocument:
                if (!looksLikeJsonText(raw)) {
                    throw new CdcParseException(
                            "binary JsonDocument cell is YDB binary JSON and cannot be converted", offset);
                }
                return PrimitiveValue.newJsonDocument(utf8(raw, offset));
            case Yson:
                return PrimitiveValue.newYson(raw);
            case TzDate:
                return PrimitiveValue.newTzDate(ProtoValue.toTzDate(utf8(raw, offset)));
            case TzDatetime:
                return PrimitiveValue.newTzDatetime(ProtoValue.toTzDatetime(utf8(raw, offset)));
            case TzTimestamp:
                return PrimitiveValue.newTzTimestamp(ProtoValue.toTzTimestamp(utf8(raw, offset)));
            case DyNumber:
            default:
                throw new CdcParseException("unsupported binary CDC type " + primitive, offset);
        }
    }

    private static Value<?> date(JsonElement element) {
        if (element.getAsJsonPrimitive().isNumber()) {
            return PrimitiveValue.newDate(element.getAsLong());
        }
        return PrimitiveValue.newDate(parseInstant(element.getAsString()));
    }

    private static Value<?> datetime(JsonElement element) {
        if (element.getAsJsonPrimitive().isNumber()) {
            return PrimitiveValue.newDatetime(element.getAsLong());
        }
        return PrimitiveValue.newDatetime(parseInstant(element.getAsString()));
    }

    private static Value<?> timestamp(JsonElement element) {
        if (element.getAsJsonPrimitive().isNumber()) {
            return PrimitiveValue.newTimestamp(element.getAsLong());
        }
        return PrimitiveValue.newTimestamp(parseInstant(element.getAsString()));
    }

    private static long signed64(JsonElement element) {
        JsonPrimitive primitive = element.getAsJsonPrimitive();
        if (primitive.isString()) {
            return new BigInteger(primitive.getAsString()).longValue();
        }
        return primitive.getAsBigInteger().longValue();
    }

    private static float floatValue(JsonElement element) {
        if (element.getAsJsonPrimitive().isString()) {
            return Float.parseFloat(element.getAsString());
        }
        return element.getAsFloat();
    }

    private static double doubleValue(JsonElement element) {
        if (element.getAsJsonPrimitive().isString()) {
            return Double.parseDouble(element.getAsString());
        }
        return element.getAsDouble();
    }

    private static Instant parseInstant(String text) {
        if (text.length() == 10) {
            return LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        return Instant.parse(text);
    }

    private static String utf8(byte[] raw, int offset) {
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            return decoder.decode(ByteBuffer.wrap(raw)).toString();
        } catch (CharacterCodingException ex) {
            throw new CdcParseException("invalid UTF-8 in binary CDC cell", offset, ex);
        }
    }

    private static boolean looksLikeJsonText(byte[] raw) {
        int index = 0;
        while (index < raw.length) {
            byte current = raw[index];
            if (current != ' ' && current != '\n' && current != '\r' && current != '\t') {
                break;
            }
            index++;
        }
        if (index >= raw.length) {
            return false;
        }
        byte marker = raw[index];
        return marker == '{' || marker == '[' || marker == '"' || marker == 't' || marker == 'f'
                || marker == 'n' || marker == '-' || (marker >= '0' && marker <= '9');
    }
}
