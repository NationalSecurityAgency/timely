package timely.store.compaction;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.store.MetricAgeOffIterator;

public class MetricCompactionStrategy extends CompactionStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(MetricCompactionStrategy.class);

    private boolean hasMinAgeOff;
    private MetricAgeOffConfiguration minAgeOffConfig;
    private boolean logOnly;
    private String filterPrefix;
    private String majcIteratorName;
    private long explicitTimeMillis;
    private boolean useExplicitTime;

    public static final String LOG_ONLY_KEY = "logonly";
    public static final String MAJC_ITERATOR_NAME_KEY = "majc.name";
    public static final String DEFAULT_MAJC_ITERATOR_NAME = "ageoffmetrics";
    public static final String AGE_OFF_PREFIX = MetricAgeOffIterator.AGE_OFF_PREFIX;
    public static final String FILTER_PREFIX_KEY = AGE_OFF_PREFIX + "filter";
    public static final String MIN_AGEOFF_KEY = AGE_OFF_PREFIX + "min";
    public static final String DEFAULT_AGEOFF_KEY_SUFFIX = MetricAgeOffIterator.DEFAULT_AGEOFF_KEY;
    public static final String DEFAULT_AGEOFF_KEY = AGE_OFF_PREFIX + DEFAULT_AGEOFF_KEY_SUFFIX;

    @Override
    public void init(Map<String,String> options) {
        // options:
        // logonly - disables compaction and will only log result
        // majciterator - the iterator name to gather age-off values
        // filter - limit the compaction to just a subset of prefixes
        // min - minimum age-off which may override the metric age-off;
        // allows more data to accumulate in tablets without being removed

        logOnly = options.containsKey(LOG_ONLY_KEY) && Boolean.parseBoolean(options.get(LOG_ONLY_KEY));
        majcIteratorName = options.getOrDefault(MAJC_ITERATOR_NAME_KEY, DEFAULT_MAJC_ITERATOR_NAME);
        filterPrefix = options.get(FILTER_PREFIX_KEY);
        if (options.containsKey(MIN_AGEOFF_KEY)) {
            long minAgeOff = Long.parseLong(options.get(MIN_AGEOFF_KEY));
            hasMinAgeOff = true;
            minAgeOffConfig = MetricAgeOffConfiguration.newFromDefaultingMinimum(minAgeOff);
        }
    }

    @Override
    public boolean shouldCompact(MajorCompactionRequest request) {
        TabletId tabletId = request.getTabletId();

        if (null == tabletId.getEndRow() || null == tabletId.getPrevEndRow()) {
            return false;
        } else if (request.getFiles().isEmpty()) {
            return false;
        }

        // check if prev/end key are both at age-off intervals
        // skip tablet if there is no prev-end key, or previous age-off is not at max;
        // other age-off will compact
        Optional<String> endName = TabletRowAdapter.decodeRowPrefix(tabletId.getEndRow());
        OptionalLong endOffset = TabletRowAdapter.decodeRowOffset(tabletId.getEndRow());
        Optional<String> prevEndName = TabletRowAdapter.decodeRowPrefix(tabletId.getPrevEndRow());

        if (!endName.isPresent() || !prevEndName.isPresent() || !endOffset.isPresent()) {
            return false;
        }

        // compute age-off from majc iterator
        // then maximize on the min-default-ageoff so that the
        // configuration with largest age-off is used
        MetricAgeOffConfiguration ageOffConfig = MetricAgeOffConfiguration.newFromRequest(request, majcIteratorName);
        if (hasMinAgeOff) {
            ageOffConfig = MetricAgeOffConfiguration.maximizeDefaultAgeOff(ageOffConfig, minAgeOffConfig);
        }

        long currentTime = useExplicitTime ? explicitTimeMillis : System.currentTimeMillis();
        long ageOffComputed = currentTime - ageOffConfig.computeAgeOff(endName.get());
        boolean shouldCompact = (ageOffComputed > endOffset.getAsLong() && endName.equals(prevEndName))
                        && (null == filterPrefix || endName.get().startsWith(filterPrefix));

        if (LOG.isDebugEnabled() && logOnly && shouldCompact) {
            LOG.debug("Tablet will be metric age-off compacted {threshold: {}, endKey: {}, offset: {}, prevKey: {}}", ageOffComputed, endName,
                            endOffset.getAsLong(), prevEndName);
        } else if (LOG.isTraceEnabled()) {
            LOG.trace("Tablet check metric compaction {threshold: {}, endKey: {}, offset: {}, prevKey: {}, result: {}}", ageOffComputed, endName,
                            endOffset.getAsLong(), prevEndName, shouldCompact);
        }

        return shouldCompact && !logOnly;
    }

    @Override
    public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
        CompactionPlan plan = null;
        if (shouldCompact(request)) {
            plan = new CompactionPlan();
            for (StoredTabletFile f : request.getFiles().keySet()) {
                plan.inputFiles.add(f);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Tablet selected for age-off: " + TabletRowAdapter.toDebugOutput(request.getTabletId()));
            }
        }

        return plan;
    }

    void setExplicitTime(long timeMillis) {
        explicitTimeMillis = timeMillis;
        useExplicitTime = true;
    }

    private static class MetricAgeOffConfiguration {

        private final PatriciaTrie<Long> ageoffs;
        private final long defaultAgeOff;

        private MetricAgeOffConfiguration(PatriciaTrie<Long> ageoffs, long defaultAgeOff) {
            this.ageoffs = ageoffs;
            this.defaultAgeOff = defaultAgeOff;
        }

        public static MetricAgeOffConfiguration maximizeDefaultAgeOff(MetricAgeOffConfiguration x, MetricAgeOffConfiguration y) {
            return (x.defaultAgeOff > y.defaultAgeOff) ? x : y;
        }

        public static MetricAgeOffConfiguration newFromDefaultingMinimum(long minAgeOff) {
            return new MetricAgeOffConfiguration(new PatriciaTrie<>(), minAgeOff);
        }

        public static MetricAgeOffConfiguration newFromRequest(MajorCompactionRequest request, String majcIteratorName) {
            Configuration config = new MapConfiguration(request.getTableProperties());
            String majcIteratorKey = Property.TABLE_ITERATOR_MAJC_PREFIX.getKey() + majcIteratorName;

            if (LOG.isTraceEnabled()) {
                LOG.trace("Using key lookup for iterator: {}", majcIteratorKey);
            }

            Configuration configAgeOff = config.subset((majcIteratorKey + ".opt"));
            if (null == configAgeOff.getString(DEFAULT_AGEOFF_KEY)) {
                throw new IllegalArgumentException(DEFAULT_AGEOFF_KEY_SUFFIX + " must be configured for  " + majcIteratorKey);
            }

            PatriciaTrie<Long> ageoffs = new PatriciaTrie<>();
            long configureMinAgeOff = Long.MAX_VALUE;
            long configureMaxAgeOff = Long.MIN_VALUE;
            @SuppressWarnings("unchecked")
            Iterator<String> keys = configAgeOff.getKeys();
            while (keys.hasNext()) {
                String k = keys.next();
                String v = configAgeOff.getString(k);
                if (k.startsWith((AGE_OFF_PREFIX))) {
                    String name = k.substring(AGE_OFF_PREFIX.length());
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Adding {} to Trie with value {}", name, Long.parseLong(v));
                    }
                    long ageoff = Long.parseLong(v);
                    configureMinAgeOff = Math.min(configureMinAgeOff, ageoff);
                    configureMaxAgeOff = Math.max(configureMaxAgeOff, ageoff);
                    ageoffs.put(name, ageoff);
                }
            }
            long defaultAgeOff = ageoffs.get(DEFAULT_AGEOFF_KEY_SUFFIX);
            return new MetricAgeOffConfiguration(ageoffs, defaultAgeOff);
        }

        public long computeAgeOff(String metricName) {
            return ageoffs.getOrDefault(metricName, defaultAgeOff);
        }
    }
}
