package tech.ydb.cdc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Little-endian helper used to build binary CDC fixtures.
 */
final class BinaryBytes {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    BinaryBytes u8(int value) {
        out.write(value & 0xff);
        return this;
    }

    BinaryBytes u16(int value) {
        out.write(value & 0xff);
        out.write((value >>> 8) & 0xff);
        return this;
    }

    BinaryBytes u32(long value) {
        u16((int) value);
        return u16((int) (value >>> 16));
    }

    BinaryBytes u64(long value) {
        u32(value);
        return u32(value >>> 32);
    }

    BinaryBytes raw(byte[] bytes) {
        try {
            out.write(bytes);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return this;
    }

    BinaryBytes textCell(String text) {
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        u32(bytes.length);
        return raw(bytes);
    }

    BinaryBytes header(int kind, int op, int flags, int keyCount, int valueCount) {
        return u8(BinaryCdcDecoder.VERSION).u8(kind).u8(op).u8(flags).u16(keyCount).u16(valueCount);
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }
}
