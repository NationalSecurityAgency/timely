package timely.server.store;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.commons.collections.buffer.BoundedFifoBuffer;

import timely.model.parse.TagListParser;

public class TagFilter extends Filter {
    private static final int DEFAULT_MAX_CACHESIZE = 10000;
    public static final String CACHE_SIZE = "maxCacheSize";
    public static final String TAGS = "tags";
    private static final PairLexicoder<Long,String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
    private static final TagListParser tagListParser = new TagListParser();
    private Map<String,Pattern> requestedTags = new LinkedHashMap<>();
    private Collection<String> tagCacheSuccess;
    private Collection<String> tagCacheFailure;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        AtomicInteger maxCacheSize = new AtomicInteger(DEFAULT_MAX_CACHESIZE);
        options.forEach((k, v) -> {
            if (k.equals(TAGS)) {
                new TagListParser().parse(v).forEach(t -> this.requestedTags.put(t.getKey(), Pattern.compile(t.getValue())));
            }
            if (k.equals(CACHE_SIZE)) {
                maxCacheSize.set(Integer.parseInt(v));
            }
        });
        // Use half of the maxCacheSize for successes and half for failures
        tagCacheSuccess = new BoundedFifoBuffer(maxCacheSize.get() / 2);
        tagCacheFailure = new BoundedFifoBuffer(maxCacheSize.get() / 2);
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
        Map<String,String> metricTags = new LinkedHashMap<>();
        tagListParser.parse(tagList).forEach(t -> metricTags.put(t.getKey(), t.getValue()));
        for (Map.Entry<String,Pattern> requestedTag : this.requestedTags.entrySet()) {
            String metricTagValue = metricTags.get(requestedTag.getKey());
            if (metricTagValue == null) {
                accept = false;
                break;
            } else {
                Pattern requestedTagPattern = requestedTag.getValue();
                if (!requestedTagPattern.matcher(metricTagValue).matches()) {
                    accept = false;
                    break;
                }
            }
        }
        if (accept) {
            this.tagCacheSuccess.add(tagList);
        } else {
            this.tagCacheFailure.add(tagList);
        }
        return accept;
    }
}
