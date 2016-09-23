package timely.model.parse;

import com.google.common.base.Splitter;
import timely.model.Tag;

import java.util.List;

public class TagParser implements Parser<Tag> {

    private static final Splitter equalSplitter = Splitter.on("=").trimResults();

    @Override
    public Tag parse(String t) {
        List<String> parts = equalSplitter.splitToList(t);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid tag format: " + t);
        }
        return new Tag(parts.get(0), parts.get(1));
    }
}
