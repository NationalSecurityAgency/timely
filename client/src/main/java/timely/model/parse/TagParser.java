package timely.model.parse;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import timely.model.Tag;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * Parses tag1=val1 into a {@code Tag} Combines {@code Tag} into {@code String}
 * of tag1=val1 NOTE: the tag and the value are both URL encoded and
 * subsequently URL decoded.
 */
public class TagParser implements Parser<Tag>, Combiner<Tag> {

    private static final Splitter equalSplitter = Splitter.on("=").trimResults();
    private static final Joiner equalsJoiner = Joiner.on("=");

    @Override
    public Tag parse(String t) {
        List<String> parts = equalSplitter.splitToList(t);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid tag format: " + t);
        }
        return new Tag(unescape(parts.get(0)), unescape(parts.get(1)));
    }

    @Override
    public String combine(Tag t) {
        return equalsJoiner.join(escape(t.getKey()), escape(t.getValue()));
    }

    private String escape(String tag) {
        try {
            return URLEncoder.encode(tag, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private String unescape(String tag) {
        try {
            return URLDecoder.decode(tag, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

}
