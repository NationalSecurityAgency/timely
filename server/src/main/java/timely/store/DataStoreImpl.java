package timely.store;

import static org.apache.accumulo.core.conf.AccumuloConfiguration.getMemoryInBytes;
import static org.apache.accumulo.core.conf.AccumuloConfiguration.getTimeInMillis;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.Server;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.model.Meta;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.RateOption;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.api.response.timeseries.SearchLookupResponse;
import timely.api.response.timeseries.SearchLookupResponse.Result;
import timely.api.response.timeseries.SuggestResponse;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Sample;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;
import timely.store.iterators.RateIterator;
import timely.store.cache.DataStoreCache;
import timely.util.MetaKeySet;

public class DataStoreImpl implements DataStore {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreImpl.class);

    private static final long METRICS_PERIOD = 30000;
    private static final Pattern REGEX_TEST = Pattern.compile("^\\w+$");

    /*
     * Pair doesn't implement Comparable
     */
    private static class MetricTagK extends Pair<String, String> implements Comparable<MetricTagK> {

        public MetricTagK(String f, String s) {
            super(f, s);
        }

        @Override
        public int compareTo(MetricTagK o) {
            int result = getFirst().compareTo(o.getFirst());
            if (result != 0) {
                return result;
            }
            return getSecond().compareTo(o.getSecond());
        }

    }

    private final Connector connector;
    private MetaCache metaCache = null;
    private final AtomicLong lastCountTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicReference<SortedMap<MetricTagK, Integer>> metaCounts = new AtomicReference<>(new TreeMap<>());
    private final String metricsTable;
    private final String metaTable;
    private final InternalMetrics internalMetrics = new InternalMetrics();
    private final Timer internalMetricsTimer = new Timer(true);
    private final int scannerThreads;
    private final long maxDownsampleMemory;
    private final BatchWriterConfig bwConfig;
    private final List<BatchWriter> writers = new ArrayList<>();
    private final ThreadLocal<BatchWriter> metaWriter = new ThreadLocal<>();
    private final ThreadLocal<BatchWriter> batchWriter = new ThreadLocal<>();
    private boolean anonAccessAllowed = false;
    private final Map<String, String> ageOff;
    private final long defaultAgeOffMilliSec;
    private DataStoreCache cache = null;

    public DataStoreImpl(Configuration conf, int numWriteThreads) throws TimelyException {

        try {
            final BaseConfiguration apacheConf = new BaseConfiguration();
            Configuration.Accumulo accumuloConf = conf.getAccumulo();
            apacheConf.setProperty("instance.name", accumuloConf.getInstanceName());
            apacheConf.setProperty("instance.zookeeper.host", accumuloConf.getZookeepers());
            final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
            final Instance instance = new ZooKeeperInstance(aconf);
            connector = instance
                    .getConnector(accumuloConf.getUsername(), new PasswordToken(accumuloConf.getPassword()));
            bwConfig = new BatchWriterConfig();
            bwConfig.setMaxLatency(getTimeInMillis(accumuloConf.getWrite().getLatency()), TimeUnit.MILLISECONDS);
            bwConfig.setMaxMemory(getMemoryInBytes(accumuloConf.getWrite().getBufferSize()) / numWriteThreads);
            bwConfig.setMaxWriteThreads(accumuloConf.getWrite().getThreads());
            scannerThreads = accumuloConf.getScan().getThreads();
            maxDownsampleMemory = accumuloConf.getScan().getMaxDownsampleMemory();
            anonAccessAllowed = conf.getSecurity().isAllowAnonymousAccess();

            metricsTable = conf.getMetricsTable();
            if (metricsTable.contains(".")) {
                final String[] parts = metricsTable.split("\\.", 2);
                final String namespace = parts[0];
                if (!connector.namespaceOperations().exists(namespace)) {
                    try {
                        LOG.info("Creating namespace " + namespace);
                        connector.namespaceOperations().create(namespace);
                    } catch (final NamespaceExistsException ex) {
                        // don't care
                    }
                }
            }
            ageOff = getAgeOff(conf);
            defaultAgeOffMilliSec = this.getAgeOffForMetric(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY);

            final Map<String, String> tableIdMap = connector.tableOperations().tableIdMap();
            if (!tableIdMap.containsKey(metricsTable)) {
                try {
                    LOG.info("Creating table " + metricsTable);
                    connector.tableOperations().create(metricsTable);
                } catch (final TableExistsException ex) {
                    // don't care
                }
            }
            this.removeAgeOffIterators(connector, metricsTable);
            this.applyAgeOffIterator(connector, metricsTable, true);

            metaTable = conf.getMetaTable();
            if (!tableIdMap.containsKey(metaTable)) {
                try {
                    LOG.info("Creating table " + metaTable);
                    connector.tableOperations().create(metaTable);
                } catch (final TableExistsException ex) {
                    // don't care
                }
            }
            this.removeAgeOffIterators(connector, metaTable);
            this.applyAgeOffIterator(connector, metaTable, false);

            internalMetricsTimer.schedule(new TimerTask() {

                @Override
                public void run() {
                    internalMetrics.getMetricsAndReset().forEach(m -> store(m));
                }

            }, METRICS_PERIOD, METRICS_PERIOD);

            this.metaCache = MetaCacheFactory.getCache(conf);
        } catch (Exception e) {
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error creating DataStoreImpl",
                    e.getMessage(), e);
        }
    }

    @Override
    public void setCache(DataStoreCache cache) {
        this.cache = cache;
    }

    @Override
    public DataStoreCache getCache() {
        return cache;
    }

    private static final EnumSet<IteratorScope> AGEOFF_SCOPES = EnumSet.allOf(IteratorScope.class);

    private void removeAgeOffIterators(Connector con, String tableName) throws Exception {
        Map<String, EnumSet<IteratorScope>> iters = con.tableOperations().listIterators(tableName);
        for (String name : iters.keySet()) {
            if (name.startsWith("ageoff")) {
                con.tableOperations().removeIterator(tableName, name, AGEOFF_SCOPES);
            }
        }
    }

    private void applyAgeOffIterator(Connector con, String tableName, boolean useIterator) throws Exception {
        IteratorSetting ageOffIteratorSettings = null;
        if (useIterator) {
            ageOffIteratorSettings = new IteratorSetting(100, "ageoff", MetricAgeOffIterator.class, this.ageOff);
        } else {
            ageOffIteratorSettings = new IteratorSetting(100, "ageoff", MetricAgeOffFilter.class, this.ageOff);
        }
        connector.tableOperations().attachIterator(tableName, ageOffIteratorSettings, AGEOFF_SCOPES);
    }

    private Map<String, String> getAgeOff(Configuration conf) {
        Map<String, String> ageOffOptions = new HashMap<>();
        conf.getMetricAgeOffDays().forEach((k, v) -> {
            String ageoff = Long.toString(v * 86400000L);
            LOG.trace("Adding age off for metric: {} of {} days", k, v);
            ageOffOptions.put(MetricAgeOffIterator.AGE_OFF_PREFIX + k, ageoff);
        });
        return ageOffOptions;
    }

    @Override
    public void store(Metric metric) {
        LOG.trace("Received Store Request for: {}", metric);

        if (cache != null) {
            cache.store(metric);
        }

        if (null == metaWriter.get()) {
            try {
                BatchWriter w = connector.createBatchWriter(metaTable, bwConfig);
                metaWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e) {
                LOG.error("Error creating meta batch writer", e);
                return;
            }
        }
        if (null == batchWriter.get()) {
            try {
                BatchWriter w = connector.createBatchWriter(metricsTable, bwConfig);
                batchWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e) {
                LOG.error("Error creating metric batch writer", e);
                return;
            }
        }

        internalMetrics.incrementMetricsReceived(1);
        List<Meta> toCache = new ArrayList<>(metric.getTags().size());
        for (final Tag tag : metric.getTags()) {
            Meta key = new Meta(metric.getName(), tag.getKey(), tag.getValue());
            if (!metaCache.contains(key)) {
                toCache.add(key);
            }
        }
        if (!toCache.isEmpty()) {
            final Set<Mutation> muts = new TreeSet<>(new Comparator<Mutation>() {

                @Override
                public int compare(Mutation o1, Mutation o2) {
                    if (o1.equals(o2)) {
                        return 0;
                    } else {
                        if (o1.hashCode() < o2.hashCode()) {
                            return -1;
                        } else {
                            return 1;
                        }
                    }
                }
            });
            MetaKeySet mks = new MetaKeySet();
            toCache.forEach(m -> mks.addAll(m.toKeys()));
            internalMetrics.incrementMetaKeysInserted(mks.size());
            muts.addAll(mks.toMutations());
            try {
                metaWriter.get().addMutations(muts);
            } catch (MutationsRejectedException e) {
                LOG.error("Unable to write to meta table", e);
                try {
                    try {
                        final BatchWriter w = metaWriter.get();
                        metaWriter.remove();
                        writers.remove(w);
                        w.close();
                    } catch (MutationsRejectedException e1) {
                        LOG.error("Error closing meta writer", e1);
                    }
                    final BatchWriter w = connector.createBatchWriter(metaTable, bwConfig);
                    metaWriter.set(w);
                    writers.add(w);
                } catch (TableNotFoundException e1) {
                    Server.fatal("Unexpected error recreating meta batch writer, shutting down Timely server", e1);
                }
            }
            metaCache.addAll(toCache);
        }
        try {
            batchWriter.get().addMutation(MetricAdapter.toMutation(metric));
            internalMetrics.incrementMetricKeysInserted(metric.getTags().size());
        } catch (MutationsRejectedException e) {
            LOG.error("Unable to write to metrics table", e);
            try {
                try {
                    final BatchWriter w = batchWriter.get();
                    batchWriter.remove();
                    writers.remove(w);
                    w.close();
                } catch (MutationsRejectedException e1) {
                    LOG.error("Error closing metric writer", e1);
                }
                final BatchWriter w = connector.createBatchWriter(metricsTable, bwConfig);
                batchWriter.set(w);
                writers.add(w);
            } catch (TableNotFoundException e1) {
                Server.fatal("Unexpected error recreating metrics batch writer, shutting down Timely server", e1);
            }
        }
    }

    private static final long FIVE_MINUTES_IN_MS = TimeUnit.MINUTES.toMillis(5);

    private void updateMetricCounts() {
        long now = System.currentTimeMillis();
        if (now - lastCountTime.get() > FIVE_MINUTES_IN_MS) {
            this.lastCountTime.set(now);
            SortedMap<MetricTagK, Integer> update = new TreeMap<>();
            for (Meta meta : this.metaCache) {
                MetricTagK key = new MetricTagK(meta.getMetric(), meta.getTagKey());
                Integer count = update.getOrDefault(key, 0);
                update.put(key, count + 1);
            }
            this.metaCounts.set(update);
        }
    }

    @Override
    public SuggestResponse suggest(SuggestRequest request) throws TimelyException {
        SuggestResponse result = new SuggestResponse();
        try {
            if (request.getType().equals("metrics")) {
                Range range;
                if (request.getQuery().isPresent()) {
                    Text start = new Text(Meta.METRIC_PREFIX + request.getQuery().get());
                    Text endRow = new Text(start);
                    endRow.append(new byte[] { (byte) 0xff }, 0, 1);
                    range = new Range(start, endRow);
                } else {
                    // kind of a hack, maybe someone wants a metric with >100
                    // 0xff bytes?
                    Text start = new Text(Meta.METRIC_PREFIX);
                    byte last = (byte) 0xff;
                    byte[] lastBytes = new byte[100];
                    Arrays.fill(lastBytes, last);
                    Text end = new Text(Meta.METRIC_PREFIX);
                    end.append(lastBytes, 0, lastBytes.length);
                    range = new Range(start, end);
                }
                Scanner scanner = connector.createScanner(metaTable, Authorizations.EMPTY);
                scanner.setRange(range);
                List<String> metrics = new ArrayList<>();
                for (Entry<Key, Value> metric : scanner) {
                    metrics.add(metric.getKey().getRow().toString().substring(Meta.METRIC_PREFIX.length()));
                    if (metrics.size() >= request.getMax()) {
                        break;
                    }
                }
                result.setSuggestions(metrics);
            }
        } catch (Exception ex) {
            LOG.error("Error during suggest: " + ex.getMessage(), ex);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during suggest: "
                    + ex.getMessage(), ex.getMessage(), ex);
        }
        return result;
    }

    @Override
    public void flush() {
        internalMetricsTimer.cancel();
        writers.forEach(w -> {
            try {
                w.close();
            } catch (final Exception ex) {
                LOG.warn("Error shutting down batchwriter", ex);
            }

        });
    }

    @Override
    public SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException {
        long startMillis = System.currentTimeMillis();
        SearchLookupResponse result = new SearchLookupResponse();
        result.setType("LOOKUP");
        result.setMetric(msg.getQuery());
        Map<String, String> tags = new TreeMap<>();
        for (Tag tag : msg.getTags()) {
            tags.put(tag.getKey(), tag.getValue());
        }
        result.setTags(tags);
        result.setLimit(msg.getLimit());
        Map<String, Pattern> tagPatterns = new HashMap<>();
        tags.forEach((k, v) -> {
            tagPatterns.put(k, Pattern.compile(v));
        });
        try {
            List<Result> resultField = new ArrayList<>();
            Scanner scanner = connector.createScanner(metaTable, Authorizations.EMPTY);
            Key start = new Key(Meta.VALUE_PREFIX + msg.getQuery());
            Key end = start.followingKey(PartialKey.ROW);
            Range range = new Range(start, end);
            scanner.setRange(range);
            tags.keySet().forEach(k -> scanner.fetchColumnFamily(new Text(k)));
            int total = 0;
            for (Entry<Key, Value> entry : scanner) {
                Meta metaEntry = Meta.parse(entry.getKey(), entry.getValue());
                if (matches(metaEntry.getTagKey(), metaEntry.getTagValue(), tagPatterns)) {
                    if (resultField.size() < msg.getLimit()) {
                        Result r = new Result();
                        r.putTag(metaEntry.getTagKey(), metaEntry.getTagValue());
                        resultField.add(r);
                    }
                    total++;
                }
            }
            result.setResults(resultField);
            result.setTotalResults(total);
            result.setTime((int) (System.currentTimeMillis() - startMillis));
        } catch (Exception ex) {
            LOG.error("Error during lookup: " + ex.getMessage(), ex);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during lookup: "
                    + ex.getMessage(), ex.getMessage(), ex);
        }
        return result;
    }

    private boolean matches(String tagk, String tagv, Map<String, Pattern> tags) {
        for (Entry<String, Pattern> entry : tags.entrySet()) {
            if (tagk.equals(entry.getKey()) && entry.getValue().matcher(tagv).matches()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<QueryResponse> query(QueryRequest msg) throws TimelyException {
        List<QueryResponse> result = new ArrayList<>();

        LOG.debug("Query request {}", msg);
        long requestedStartTs = msg.getStart();
        long requestedEndTs = msg.getEnd();
        StringBuilder metricList = new StringBuilder();

        try {
            long numResults = 0;
            long now = System.currentTimeMillis();
            for (SubQuery query : msg.getQueries()) {
                Map<Set<Tag>, List<Aggregation>> allSeries = new HashMap<>();
                String metric = query.getMetric();
                if (metricList.length() > 0) {
                    metricList.append(",");
                }
                metricList.append(metric);
                Map<Set<Tag>, List<Aggregation>> cachedMetrics = new HashMap<>();
                long oldestTimestampFromCache = 0;

                if (cache != null) {
                    long oldestCacheTimestamp = cache.getOldestTimestamp(query.getMetric());
                    if (requestedEndTs >= oldestCacheTimestamp) {
                        cachedMetrics = cache.subquery(msg, query);
                        allSeries.putAll(cachedMetrics);
                    }
                    oldestTimestampFromCache = Math.max(oldestCacheTimestamp,
                            System.currentTimeMillis() - cache.getAgeOffForMetric(query.getMetric()) + 1);
                }

                if (cachedMetrics.isEmpty() || requestedStartTs < oldestTimestampFromCache) {
                    // we have already searched from oldestTimestampFromCache to
                    // requestedEndTs
                    long endTs = (oldestTimestampFromCache == 0) ? requestedEndTs : oldestTimestampFromCache - 1;

                    // Reset the start timestamp for the query to the
                    // beginning of the downsample period based on the epoch
                    long downsample = DownsampleIterator.getDownsamplePeriod(query);
                    LOG.trace("Downsample period {}", downsample);
                    long startOfFirstPeriod = requestedStartTs - (requestedStartTs % downsample);
                    long endDistanceFromDownSample = endTs % downsample;
                    long endOfLastPeriod = (endDistanceFromDownSample > 0 ? endTs + downsample
                            - endDistanceFromDownSample : endTs);

                    if (endOfLastPeriod > startOfFirstPeriod) {
                        BatchScanner scanner = null;
                        try {
                            scanner = connector.createBatchScanner(metricsTable, getSessionAuthorizations(msg),
                                    scannerThreads);
                            List<String> tagOrder = prioritizeTags(query.getMetric(), query.getTags());
                            Map<String, String> orderedTags = orderTags(tagOrder, query.getTags());
                            Set<Tag> colFamValues = getColumnFamilies(metric, orderedTags);
                            List<Range> ranges = getQueryRanges(metric, startOfFirstPeriod, endOfLastPeriod,
                                    colFamValues);
                            scanner.setRanges(ranges);
                            setQueryColumns(scanner, metric, orderedTags, colFamValues);

                            if (query.isRate()) {
                                LOG.trace("Adding rate iterator");
                                IteratorSetting rate = new IteratorSetting(499, RateIterator.class);
                                RateIterator.setRateOptions(rate, query.getRateOptions());
                                scanner.addScanIterator(rate);
                            }

                            Class<? extends Aggregator> daggClass = DownsampleIterator.getDownsampleAggregator(query);
                            if (daggClass == null) {
                                // we should always have a downsample iterator
                                // in the stack.
                                throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                        "Error during query: programming error", "daggClass == null");
                            } else {
                                LOG.trace("Downsample Aggregator type {}", daggClass.getSimpleName());
                                IteratorSetting is = new IteratorSetting(500, DownsampleIterator.class);
                                DownsampleIterator.setDownsampleOptions(is, startOfFirstPeriod, endOfLastPeriod,
                                        downsample, maxDownsampleMemory, daggClass.getName());
                                scanner.addScanIterator(is);
                            }

                            Class<? extends Aggregator> aggClass = getAggregator(query);
                            // the aggregation iterator is optional
                            if (aggClass != null) {
                                LOG.trace("Aggregator type {}", aggClass.getSimpleName());
                                IteratorSetting is = new IteratorSetting(501, AggregationIterator.class);
                                AggregationIterator.setAggregationOptions(is, query.getTags(), aggClass.getName());
                                scanner.addScanIterator(is);
                            }

                            // tag -> array of results by period starting at
                            // start
                            for (Entry<Key, Value> encoded : scanner) {
                                // we can decode the value as a Map<Set<Tag>,
                                // Aggregation> even if an AggregationIterator
                                // is not used because Downsample is a subclass
                                // of Aggregation
                                Map<Set<Tag>, Aggregation> samples = AggregationIterator
                                        .decodeValue(encoded.getValue());
                                for (Entry<Set<Tag>, Aggregation> entry : samples.entrySet()) {
                                    Set<Tag> key = new HashSet<>();
                                    for (Tag tag : entry.getKey()) {
                                        if (query.getTags().keySet().contains(tag.getKey())) {
                                            key.add(tag);
                                        }
                                    }
                                    List<Aggregation> aggregations = allSeries.getOrDefault(key, new ArrayList<>());
                                    aggregations.add(entry.getValue());
                                    allSeries.put(key, aggregations);
                                }
                            }
                            LOG.trace("allSeries: {}", allSeries);
                        } finally {
                            if (scanner != null) {
                                scanner.close();
                            }
                        }
                    }
                }

                // TODO groupby here?
                long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                for (Entry<Set<Tag>, List<Aggregation>> entry : allSeries.entrySet()) {
                    numResults += entry.getValue().size();
                    result.add(convertToQueryResponse(query, entry.getKey(), entry.getValue(), tsDivisor));
                }
            }
            LOG.debug("Query time: {} duration: {} metrics: {} results: {}", (System.currentTimeMillis() - now),
                    (requestedEndTs - requestedStartTs), metricList.toString(), numResults);
            internalMetrics.addQueryResponse(result.size(), (System.currentTimeMillis() - now));
            return result;
        } catch (ClassNotFoundException | IOException | TableNotFoundException ex) {
            LOG.error("Error during query: " + ex.getMessage(), ex);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + ex.getMessage(), ex.getMessage(), ex);
        }
    }

    private Map<String, String> orderTags(List<String> tagOrder, Map<String, String> tags) {
        Map<String, String> order = new LinkedHashMap<>(tags.size());
        tagOrder.forEach(t -> order.put(t, tags.get(t)));
        if (tagOrder.size() > tags.size()) {
            tags.entrySet().forEach(k -> {
                if (!tagOrder.contains(k.getKey())) {
                    order.put(k.getKey(), k.getValue());
                }
            });
        }
        return order;
    }

    /**
     *
     * @param query
     * @return ordered list of most specific to least specific tags in the query
     */
    private List<String> prioritizeTags(String metric, Map<String, String> tags) {
        // trivial cases
        if (tags == null || tags.isEmpty()) {
            return Collections.emptyList();
        }
        if (tags.size() == 1) {
            return Collections.singletonList(tags.keySet().iterator().next());
        }
        // favor tags with fewer values
        Map<String, Integer> priority = new HashMap<>();
        // Count matching tags
        updateMetricCounts();
        for (Entry<String, String> entry : tags.entrySet()) {
            String tagk = entry.getKey();
            String tagv = entry.getValue();
            if (!isTagValueRegex(tagv)) {
                MetricTagK start = new MetricTagK(metric, tagk);
                int count = 0;
                for (Entry<MetricTagK, Integer> metricCount : metaCounts.get().tailMap(start).entrySet()) {
                    Pair<String, String> metricTagk = metricCount.getKey();
                    if (!metricTagk.getFirst().equals(metric) || !metricTagk.getSecond().startsWith(tagk)) {
                        break;
                    } else {
                        count += metricCount.getValue();
                    }
                }
                priority.put(tagk, count);
            } else {
                priority.put(tagk, Integer.MAX_VALUE);
            }
        }
        List<String> result = new ArrayList<>(tags.keySet());
        Collections.sort(result, new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                // greater count lowers priority
                return priority.get(o1).intValue() - priority.get(o2).intValue();
            }
        });
        LOG.trace("Tag priority {}", result);
        return result;
    }

    private QueryResponse convertToQueryResponse(SubQuery query, Set<Tag> tags, Collection<Aggregation> values,
            long tsDivisor) {
        QueryResponse response = new QueryResponse();
        response.setMetric(query.getMetric());
        for (Tag tag : tags) {
            response.putTag(tag.getKey(), tag.getValue());
        }
        RateOption rateOptions = query.getRateOptions();
        Aggregation combined = Aggregation.combineAggregation(values, rateOptions);
        for (Sample entry : combined) {
            long ts = entry.timestamp / tsDivisor;
            response.putDps(Long.toString(ts), entry.value);
        }
        LOG.trace("Created query response {}", response);
        return response;
    }

    private boolean isTagValueRegex(String value) {
        return !REGEX_TEST.matcher(value).matches();
    }

    public Set<Tag> getColumnFamilies(String metric, Map<String, String> requestedTags) throws TableNotFoundException {

        Map<String, String> tags = (requestedTags == null) ? new LinkedHashMap<>() : requestedTags;
        LOG.trace("Looking for requested tags: {}", tags);
        Scanner meta = connector.createScanner(metaTable, Authorizations.EMPTY);
        Text start = new Text(Meta.VALUE_PREFIX + metric);
        Text end = new Text(Meta.VALUE_PREFIX + metric + "\\x0000");
        end.append(new byte[] { (byte) 0xff }, 0, 1);
        meta.setRange(new Range(start, end));
        // Only look for the meta entries that match our tags, if any
        boolean onlyFirstRow = false;
        Entry<String, String> first = null;
        // Set the columns on the meta scanner based on the first tag
        // in the set of tags passed in the query. If no tags are present
        // then we are only going to return the first tag name present in the
        // meta table.
        Iterator<Entry<String, String>> tagIter = tags.entrySet().iterator();
        if (tagIter.hasNext()) {
            first = tagIter.next();
            if (isTagValueRegex(first.getValue())) {
                meta.fetchColumnFamily(new Text(first.getKey()));
            } else {
                meta.fetchColumn(new Text(first.getKey()), new Text(first.getValue()));
            }
        } else {
            // grab all of the values found for the first tag for the metric
            onlyFirstRow = true;
        }
        final boolean ONLY_RETURN_FIRST_TAG = onlyFirstRow;
        Iterator<Entry<Key, Value>> iter = meta.iterator();
        Iterator<Pair<String, String>> knownKeyValues = new Iterator<Pair<String, String>>() {

            Text firstTag = null;
            Text tagName = null;
            Text tagValue = null;

            @Override
            public boolean hasNext() {
                if (iter.hasNext()) {
                    Entry<Key, Value> metaEntry = iter.next();
                    if (null == firstTag) {
                        firstTag = metaEntry.getKey().getColumnFamily();
                    }
                    tagName = metaEntry.getKey().getColumnFamily();
                    tagValue = metaEntry.getKey().getColumnQualifier();
                    LOG.trace("Found tag entry {}={}", tagName, tagValue);

                    if (ONLY_RETURN_FIRST_TAG && !tagName.equals(firstTag)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }

            @Override
            public Pair<String, String> next() {
                LOG.trace("Returning tag {}={}", tagName, tagValue);
                return new Pair<>(tagName.toString(), tagValue.toString());
            }
        };
        // Expand the list of tags in the meta table for this metric that
        // matches
        // the pattern of the first tag in the query. The resulting set of tags
        // will be used to fetch specific columns from the metric table.
        return expandTagValues(first, knownKeyValues);
    }

    private void setQueryColumns(ScannerBase scanner, String metric, Map<String, String> tags, Set<Tag> colFamValues)
            throws TimelyException {

        if (colFamValues.size() == 0) {
            throw new TimelyException(HttpResponseStatus.BAD_REQUEST.code(), "No matching tags", "No tags were found "
                    + " that matched the submitted tags. Please fix and retry");
        }
        LOG.trace("Found matching tags: {}", colFamValues);
        for (Tag tag : colFamValues) {
            Text colf = new Text(tag.getKey() + "=" + tag.getValue());
            scanner.fetchColumnFamily(colf);
            LOG.trace("Fetching metric table column family: {}", colf);
        }
        // Add the regular expression to filter the other tags
        int priority = 100;
        Iterator<Entry<String, String>> tagIter = tags.entrySet().iterator();
        // skip over first tag which was already expanded into colFamValues
        if (tagIter.hasNext()) {
            tagIter.next();
        }
        Entry<String, String> tag = null;
        while (tagIter.hasNext()) {
            tag = tagIter.next();
            LOG.trace("Adding regex filter for tag {}", tag);
            StringBuffer pattern = new StringBuffer();
            pattern.append("(^.*\\x00|.*,)");
            pattern.append(tag.getKey());
            pattern.append("=");
            pattern.append(tag.getValue());
            pattern.append("(,.*|$)");

            IteratorSetting setting = new IteratorSetting(priority++, tag.getKey() + " tag filter", RegExFilter.class);
            LOG.trace("Using {} additional filter on tag: {}", pattern, tag.getKey());
            RegExFilter.setRegexs(setting, null, null, pattern.toString(), null, false, true);
            scanner.addScanIterator(setting);
        }
    }

    private Set<Tag> expandTagValues(Entry<String, String> firstTag, Iterator<Pair<String, String>> knownKeyValues) {
        Set<Tag> result = new HashSet<>();
        Matcher matcher = null;
        if (null != firstTag && isTagValueRegex(firstTag.getValue())) {
            matcher = Pattern.compile(firstTag.getValue()).matcher("");
        }
        while (knownKeyValues.hasNext()) {
            Pair<String, String> knownKeyValue = knownKeyValues.next();
            if (firstTag == null) {
                LOG.trace("Adding tag {}={}", knownKeyValue.getFirst(), knownKeyValue.getSecond());
                result.add(new Tag(knownKeyValue.getFirst(), knownKeyValue.getSecond()));
            } else {
                LOG.trace("Testing requested tag {}={}", firstTag.getKey(), firstTag.getValue());
                if (firstTag.getKey().equals(knownKeyValue.getFirst())) {
                    if (null != matcher) {
                        matcher.reset(knownKeyValue.getSecond());
                        if (matcher.matches()) {
                            LOG.trace("Adding tag {}={}", knownKeyValue.getFirst(), knownKeyValue.getSecond());
                            result.add(new Tag(knownKeyValue.getFirst(), knownKeyValue.getSecond()));
                        }
                    } else {
                        LOG.trace("Adding tag {}={}", knownKeyValue.getFirst(), knownKeyValue.getSecond());
                        result.add(new Tag(knownKeyValue.getFirst(), knownKeyValue.getSecond()));
                    }
                }
            }
        }
        return result;
    }

    public List<Range> getQueryRanges(String metric, long start, long end, Set<Tag> colFamValues) {
        List<Range> ranges = new ArrayList<>();
        long beginRangeRounded = MetricAdapter.roundTimestampToLastHour(start);
        if (colFamValues.isEmpty()) {
            final byte[] start_row = MetricAdapter.encodeRowKey(metric, beginRangeRounded);
            Key startKey = new Key(new Text(start_row));
            LOG.trace("Start key for metric {} and time {} is {}", metric, beginRangeRounded, startKey.toStringNoTime());
            final byte[] end_row = MetricAdapter.encodeRowKey(metric, beginRangeRounded);
            Key endKey = new Key(new Text(end_row));
            LOG.trace("End key for metric {} and time {} is {}", metric, MetricAdapter.roundTimestampToNextHour(end),
                    endKey.toStringNoTime());
            Range range = new Range(startKey, true, endKey, false);
            LOG.trace("Set query range to {}", range);
            ranges.add(range);
        } else {
            try {
                long lastBeginRangeRounded = MetricAdapter.roundTimestampToLastHour(end);
                while (beginRangeRounded <= lastBeginRangeRounded) {
                    // use end timestamp of begin + 1 hour and one msec
                    // except the last range where we use end + 1 msec
                    long endRangeTimestamp = (beginRangeRounded == lastBeginRangeRounded) ? end + 1 : beginRangeRounded
                            + (1000 * 60 * 60) + 1;
                    long beginRangeTimestamp = (beginRangeRounded == MetricAdapter.roundTimestampToLastHour(start)) ? start
                            : beginRangeRounded;
                    for (Tag t : colFamValues) {
                        final byte[] start_row = MetricAdapter.encodeRowKey(metric, beginRangeRounded);
                        Key startKey = new Key(new Text(start_row), new Text(t.join()
                                .getBytes(Charset.forName("UTF-8"))), new Text(MetricAdapter.encodeColQual(
                                beginRangeTimestamp, "")), new Text(new byte[0]), beginRangeTimestamp);
                        LOG.trace("Start key for metric {} and time {} is {}", metric, beginRangeTimestamp,
                                startKey.toStringNoTime());
                        final byte[] end_row = MetricAdapter.encodeRowKey(metric, beginRangeRounded);
                        Key endKey = new Key(new Text(end_row), new Text(t.join().getBytes(Charset.forName("UTF-8"))),
                                new Text(MetricAdapter.encodeColQual(endRangeTimestamp, "")), new Text(new byte[0]),
                                endRangeTimestamp);
                        LOG.trace("End key for metric {} and time {} is {}", metric, beginRangeTimestamp,
                                endKey.toStringNoTime());
                        Range range = new Range(startKey, true, endKey, false);
                        LOG.trace("Set query range to {}", range);
                        ranges.add(range);
                    }
                    beginRangeRounded += (1000 * 60 * 60); // add an hour
                }
            } catch (Exception e) {
                LOG.error("Error creating query ranges", e);
            }
        }

        return ranges;
    }

    private Class<? extends Aggregator> getAggregator(SubQuery query) {
        return Aggregator.getAggregator(query.getAggregator());
    }

    private Authorizations getSessionAuthorizations(AuthenticatedRequest request) {
        return getSessionAuthorizations(request.getSessionId());
    }

    private Authorizations getSessionAuthorizations(String sessionId) {
        if (anonAccessAllowed) {
            if (StringUtils.isEmpty(sessionId)) {
                return Authorizations.EMPTY;
            } else {
                Authorizations auths = AuthCache.getAuthorizations(sessionId);
                if (null == auths) {
                    auths = Authorizations.EMPTY;
                }
                return auths;
            }
        } else {
            if (StringUtils.isEmpty(sessionId)) {
                throw new IllegalArgumentException("session id cannot be null");
            } else {
                Authorizations auths = AuthCache.getAuthorizations(sessionId);
                if (null == auths) {
                    throw new IllegalStateException("No auths found for sessionId: " + sessionId);
                }
                return auths;
            }
        }
    }

    public long getAgeOffForMetric(String metricName) {
        String age = this.ageOff.get(MetricAgeOffIterator.AGE_OFF_PREFIX + metricName);
        if (null == age) {
            return this.defaultAgeOffMilliSec;
        } else {
            return Long.parseLong(age);
        }
    }

    public Scanner createScannerForMetric(String sessionId, String metric, Map<String, String> tags, long startTime,
            long endTime, int lag, int scannerBatchSize, int scannerReadAhead) throws TimelyException {
        try {
            Authorizations auths = null;
            try {
                auths = getSessionAuthorizations(sessionId);
            } catch (NullPointerException npe) {
                // Session id being used for metric scanner, but session Id does
                // not exist in Auth Cache. Use Empty auths if anonymous access
                // allowed
                if (anonAccessAllowed) {
                    auths = Authorizations.EMPTY;
                } else {
                    throw npe;
                }
            }
            LOG.debug("Creating metric scanner for session: {} with auths: {}", sessionId, auths);
            Scanner s = connector.createScanner(this.metricsTable, auths);
            if (null == metric) {
                throw new IllegalArgumentException("metric name must be specified");
            }
            if (tags == null) {
                tags = new LinkedHashMap<>();
            }
            List<String> tagOrder = prioritizeTags(metric, tags);
            Map<String, String> orderedTags = orderTags(tagOrder, tags);
            Set<Tag> colFamValues = getColumnFamilies(metric, orderedTags);
            setQueryColumns(s, metric, orderedTags, colFamValues);
            s.setBatchSize(scannerBatchSize);
            s.setReadaheadThreshold(scannerReadAhead);
            return s;
        } catch (IllegalArgumentException | TableNotFoundException ex) {
            LOG.error("Error during lookup: " + ex.getMessage(), ex);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during lookup: "
                    + ex.getMessage(), ex.getMessage(), ex);
        }
    }

}
