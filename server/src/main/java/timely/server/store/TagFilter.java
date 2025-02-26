package timely.server.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.commons.lang3.StringUtils;

import timely.model.Tag;
import timely.model.parse.TagListParser;
import timely.model.parse.TagParser;

public class TagFilter extends Filter {
    private static final int DEFAULT_MAX_CACHESIZE = 10000;
    public static final String CACHE_SIZE = "maxCacheSize";
    public static final String TAGS = "tags";
    private static final PairLexicoder<Long,String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
    private static final TagParser tagParser = new TagParser();
    private static final TagListParser tagListParser = new TagListParser();
    private Map<String,Pattern> requestedTags = new LinkedHashMap<>();
    private Set<String> tagCacheSuccess = new HashSet<>();
    private Set<String> tagCacheFailure = new HashSet<>();
    private AtomicInteger maxCacheSize = new AtomicInteger(DEFAULT_MAX_CACHESIZE);

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        options.forEach((k, v) -> {
            if (k.equals(TAGS)) {
                Map<String,String> tags = deserializeTags(v);
                for (Map.Entry<String,String> e : tags.entrySet()) {
                    requestedTags.put(e.getKey(), Pattern.compile(e.getValue()));
                }
            }
            if (k.equals(CACHE_SIZE)) {
                // may not be less than 1000
                maxCacheSize.set(Math.max(1000, Integer.parseInt(v)));
            }
        });
    }

    // use a \0 separator instead of commas (as in TagListParser) because regex strings may contain commas
    public static Map<String,String> deserializeTags(String tagString) {
        Map<String,String> tags = new LinkedHashMap<>();
        Arrays.stream(tagString.split("\0")).forEach(s -> {
            Tag t = tagParser.parse(s);
            tags.put(t.getKey(), t.getValue());
        });
        return tags;
    }

    // use a \0 separator instead of commas (as in TagListParser) because regex strings may contain commas
    public static String serializeTags(Map<String,String> tags) {
        List<String> tagList = new ArrayList<>();
        for (Map.Entry<String,String> e : tags.entrySet()) {
            tagList.add(tagParser.combine(new Tag(e.getKey(), e.getValue())));
        }
        return StringUtils.join(tagList, "\0");
    }

    @Override
    public boolean accept(Key k, Value v) {
        if (this.requestedTags.isEmpty()) {
            return true;
        }
        ComparablePair<Long,String> cq = colQualCoder.decode(k.getColumnQualifier().getBytes());
        String tagList = cq.getSecond();
        if (this.tagCacheSuccess.contains(tagList)) {
            // fast success if this list of tags has previously passed
            return true;
        } else if (this.tagCacheFailure.contains(tagList)) {
            // fast failure if this list of tags has previously failed
            return false;
        }

        boolean accept = true;
        Map<String,String> currentMetricTags = new LinkedHashMap<>();
        tagListParser.parse(tagList).forEach(t -> currentMetricTags.put(t.getKey(), t.getValue()));
        for (Map.Entry<String,Pattern> requestedTag : this.requestedTags.entrySet()) {
            String currentTagValue = currentMetricTags.get(requestedTag.getKey());
            if (currentTagValue == null) {
                accept = false;
                break;
            } else {
                Pattern requestedTagValuePattern = requestedTag.getValue();
                if (!requestedTagValuePattern.matcher(currentTagValue).matches()) {
                    accept = false;
                    break;
                }
            }
        }
        if (accept) {
            success(tagList);
        } else {
            failure(tagList);
        }
        return accept;
    }

    // maintain a HashSet for quick lookup and a Dequeue to
    private void success(String tag) {
        if (tagCacheSuccess.size() >= maxCacheSize.get()) {
            // delete 10% of the entries
            tagCacheSuccess = tagCacheSuccess.stream().skip(maxCacheSize.get() / 10).collect(Collectors.toSet());
        }
        tagCacheSuccess.add(tag);
    }

    private void failure(String tag) {
        if (tagCacheFailure.size() >= maxCacheSize.get()) {
            // delete 10% of the entries
            tagCacheFailure = tagCacheFailure.stream().skip(maxCacheSize.get() / 10).collect(Collectors.toSet());
        }
        tagCacheFailure.add(tag);
    }

    protected void setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize.set(maxCacheSize);
    }
}
