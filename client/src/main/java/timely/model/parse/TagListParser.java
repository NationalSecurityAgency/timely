package timely.model.parse;

import com.google.common.base.Splitter;
import timely.model.Tag;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses tag1=val1,tag2=val2,tag3=val3 into a {@code List} of {@code Tag}
 */
public class TagListParser implements Parser<List<Tag>> {

    private static final Splitter commaSplitter = Splitter.on(",").omitEmptyStrings();
    private static final TagParser tagParser = new TagParser();

    @Override
    public List<Tag> parse(String t) {
        ArrayList<Tag> tags = new ArrayList<>();
        commaSplitter.splitToList(t).forEach(p -> tags.add(tagParser.parse(p)));
        return tags;
    }
}
