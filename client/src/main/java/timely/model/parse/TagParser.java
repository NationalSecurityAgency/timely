package timely.model.parse;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import timely.model.Tag;

/**
 * Parses tag1=val1 into a {@code Tag} Combines {@code Tag} into {@code String}
 * of tag1=val1
 */
public class TagParser implements Parser<Tag>, Combiner<Tag> {

    private static final Splitter equalSplitter = Splitter.on("=").limit(2).trimResults();
    private static final Joiner equalsJoiner = Joiner.on("=");

    @Override
    public Tag parse(String t) {
        List<String> parts = equalSplitter.splitToList(t);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid tag format: " + t);
        }
        return new Tag(parts.get(0), parts.get(1));
    }

    @Override
    public String combine(Tag t) {
        return equalsJoiner.join(t.getKey(), t.getValue());
    }

}
