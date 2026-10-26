package tech.ydb.cdc;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

/**
 * Column-name order used by the binary CDC format.
 * The server sorts non-key names as raw UTF-8 bytes.
 */
final class CdcNames {
    static final Comparator<String> UTF8 = new Comparator<String>() {
        @Override
        public int compare(String left, String right) {
            byte[] a = left.getBytes(StandardCharsets.UTF_8);
            byte[] b = right.getBytes(StandardCharsets.UTF_8);
            int length = Math.min(a.length, b.length);
            for (int i = 0; i < length; i++) {
                int diff = (a[i] & 0xff) - (b[i] & 0xff);
                if (diff != 0) {
                    return diff;
                }
            }
            return a.length - b.length;
        }
    };

    private CdcNames() {
    }
}
