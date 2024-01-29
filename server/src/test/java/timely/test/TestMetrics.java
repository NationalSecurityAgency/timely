package timely.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.google.common.collect.Lists;

import timely.model.Metric;
import timely.model.Tag;

public class TestMetrics {

    private final List<String> prefixes;
    private final List<String> suffixes;
    private final List<Tag> tags;

    public TestMetrics() {
        prefixes = Lists.newArrayList("sys", "statsd");
        suffixes = createRandomSuffixes();
        tags = new ArrayList<>(2);
        tags.add(new Tag("hostname", "localhost"));
        tags.add(new Tag("ip", "127.0.0.1"));
    }

    public Metric randomMetric(long timestamp) {
        int prefixIdx = RandomUtils.nextInt(0, prefixes.size());
        int suffixIdx = RandomUtils.nextInt(0, suffixes.size());
        // @formatter:off
        return Metric.newBuilder()
                .name(prefixes.get(prefixIdx) + "." + suffixes.get(suffixIdx))
                .tags(tags)
                .value(timestamp, RandomUtils.nextDouble(0, 1.0))
                .build();
    }

    private static List<String> createRandomSuffixes() {
        int SUFFIX_MIN = 5;
        int SUFFIX_MAX = 10;
        int SUFFIX_NUM = 4;
        List<String> list = new ArrayList<>(SUFFIX_NUM);
        for (int i = 0; i < SUFFIX_NUM; i++) {
            list.add(RandomStringUtils.randomAlphabetic(SUFFIX_MIN, SUFFIX_MAX).toLowerCase());
        }
        return list;
    }

}
