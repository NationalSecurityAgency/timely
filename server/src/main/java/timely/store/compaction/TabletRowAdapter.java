package timely.store.compaction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.StringJoiner;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils;
import org.apache.accumulo.core.data.TabletId;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletRowAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TabletRowAdapter.class);
    private static final Lexicoder<String> PREFIX_DECODER = new StringLexicoder();
    private static final Lexicoder<Long> OFFSET_DECODER = new LongLexicoder();

    private static final byte DELIMETER = 0x00;
    private static final int MIN_OFFSET_LEN = 9;

    public static OptionalLong decodeRowOffset(Text row) {
        OptionalLong offset = OptionalLong.empty();
        byte[] bytes = row.getBytes();
        int delimidx = findRowDelimiterIndex(bytes);
        if (delimidx > 0) {
            try {
                byte[] buffer = Arrays.copyOfRange(bytes, delimidx + 1, bytes.length);
                buffer = ByteUtils.unescape(buffer);

                // key might not have all bytes for long decoding
                // ensure of sufficient length
                int extendBy = (MIN_OFFSET_LEN - (buffer.length));
                if (extendBy > 0) {
                    buffer = ArrayUtils.addAll(buffer, new byte[extendBy]);
                }

                long decodedOffset = OFFSET_DECODER.decode(buffer);
                if (decodedOffset > 0) {
                    offset = OptionalLong.of(decodedOffset);
                } else if (LOG.isTraceEnabled()) {
                    Optional<String> prefix = decodeRowPrefix(row);
                    LOG.trace(
                            "Delimiter identified but offset could not parse { prefix: {}, byte-len: {}, "
                                    + "offset-len: {}, offset-bytes: {}, row-bytes: {} }",
                            prefix, bytes.length, buffer.length, Hex.encodeHex(buffer), Hex.encodeHex(bytes));
                }
            } catch (IllegalArgumentException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Unable to parse offset: " + e.getMessage(), e);
                }
            }
        }

        return offset;
    }

    public static Optional<String> decodeRowPrefix(Text row) {
        byte[] bytes = row.getBytes();
        int delimidx = findRowDelimiterIndex(bytes);
        byte[] prefixBytes = (delimidx >= 0) ? Arrays.copyOfRange(bytes, 0, delimidx) : bytes;
        Optional<String> tabletName = Optional.empty();
        try {
            tabletName = Optional.of(PREFIX_DECODER.decode(ByteUtils.unescape(prefixBytes)));
        } catch (IllegalArgumentException e) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Unable to parse offset: " + e.getMessage(), e);
            }
        }

        return tabletName;
    }

    public static String toDebugOutput(TabletId tid) {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        Text endRow = tid.getEndRow();
        if (null != endRow) {
            Optional<String> prefix = decodeRowPrefix(endRow);
            OptionalLong offset = decodeRowOffset(endRow);
            prefix.ifPresent(s -> joiner.add("prefix: " + s));
            if (offset.isPresent()) {
                LocalDateTime date = Instant.ofEpochMilli(offset.getAsLong()).atZone(ZoneId.of("UTC"))
                        .toLocalDateTime();
                joiner.add("date: " + date.toString());
                joiner.add("millis: " + offset.getAsLong());
            }
        }

        return joiner.toString();
    }

    private static int findRowDelimiterIndex(byte[] bytes) {
        int delimidx = -1;
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] == DELIMETER) {
                delimidx = i;
                break;
            }
        }
        return delimidx;
    }
}
