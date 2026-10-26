package tech.ydb.cdc;

/**
 * A CDC payload could not be decoded.
 */
public class CdcParseException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    private final int offset;

    public CdcParseException(String message, int offset) {
        super(message);
        this.offset = offset;
    }

    public CdcParseException(String message, int offset, Throwable cause) {
        super(message, cause);
        this.offset = offset;
    }

    /**
     * @return byte offset inside the payload where decoding stopped
     */
    public int getOffset() {
        return offset;
    }
}
