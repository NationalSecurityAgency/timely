package timely.store;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.conf.AccumuloConfiguration.getMemoryInBytes;
import static org.apache.accumulo.core.conf.AccumuloConfiguration.getTimeInMillis;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
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
import timely.api.AuthenticatedRequest;
import timely.api.model.Meta;
import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.QueryRequest.RateOption;
import timely.api.query.request.QueryRequest.SubQuery;
import timely.api.query.request.SearchLookupRequest;
import timely.api.query.request.SuggestRequest;
import timely.api.query.response.QueryResponse;
import timely.api.query.response.SearchLookupResponse;
import timely.api.query.response.SearchLookupResponse.Result;
import timely.api.query.response.SuggestResponse;
import timely.api.query.response.TimelyException;
import timely.auth.AuthCache;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.iterators.DownsampleIterator;
import timely.util.MetaKeySet;

public class DataStoreImpl implements DataStore {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreImpl.class);

    private static final long METRICS_PERIOD = 30000;
    private static final long DEFAULT_DOWNSAMPLE_MS = 60000;

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

    Configuration conf;
    private final Connector connector;
    private MetaCache metaCache = null;
    private final AtomicLong lastCountTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicReference<SortedMap<MetricTagK, Integer>> metaCounts = new AtomicReference<>(new TreeMap<>());
    private final String metricsTable;
    private final String metaTable;
    private final InternalMetrics internalMetrics = new InternalMetrics();
    private final Timer internalMetricsTimer = new Timer(true);
    private final int scannerThreads;
    private final BatchWriterConfig bwConfig;
    private final List<BatchWriter> writers = new ArrayList<>();
    private final ThreadLocal<BatchWriter> metaWriter = new ThreadLocal<>();
    private final ThreadLocal<BatchWriter> batchWriter = new ThreadLocal<>();
    private boolean anonAccessAllowed = false;

    public DataStoreImpl(Configuration conf, int numWriteThreads) throws TimelyException {

        try {
            final BaseConfiguration apacheConf = new BaseConfiguration();
            apacheConf.setProperty("instance.name", conf.get(Configuration.INSTANCE_NAME));
            apacheConf.setProperty("instance.zookeeper.host", conf.get(Configuration.ZOOKEEPERS));
            final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
            final Instance instance = new ZooKeeperInstance(aconf);
            final byte[] passwd = conf.get(Configuration.PASSWORD).getBytes(UTF_8);
            connector = instance.getConnector(conf.get(Configuration.USERNAME), new PasswordToken(passwd));
            bwConfig = new BatchWriterConfig();
            bwConfig.setMaxLatency(getTimeInMillis(conf.get(Configuration.MAX_LATENCY)), TimeUnit.MILLISECONDS);
            bwConfig.setMaxMemory(getMemoryInBytes(conf.get(Configuration.WRITE_BUFFER_SIZE)) / numWriteThreads);
            bwConfig.setMaxWriteThreads(Integer.parseInt(conf.get(Configuration.WRITE_THREADS)));
            scannerThreads = Integer.parseInt(conf.get(Configuration.SCANNER_THREADS));
            anonAccessAllowed = conf.getBoolean(Configuration.ALLOW_ANONYMOUS_ACCESS);

            String ageoff = Long.toString(Integer.parseInt(conf.get(Configuration.METRICS_AGEOFF_DAYS)) * 86400000L);
            Map<String, String> ageOffOptions = new HashMap<>();
            ageOffOptions.put("ttl", ageoff);
            IteratorSetting ageOffIteratorSettings = new IteratorSetting(100, "ageoff", AgeOffFilter.class,
                    ageOffOptions);
            EnumSet<IteratorScope> ageOffIteratorScope = EnumSet.allOf(IteratorScope.class);

            metricsTable = conf.get(Configuration.METRICS_TABLE);
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
            final Map<String, String> tableIdMap = connector.tableOperations().tableIdMap();
            if (!tableIdMap.containsKey(metricsTable)) {
                try {
                    LOG.info("Creating table " + metricsTable);
                    connector.tableOperations().create(metricsTable);
                    connector.tableOperations().attachIterator(metricsTable, ageOffIteratorSettings,
                            ageOffIteratorScope);
                } catch (final TableExistsException ex) {
                    // don't care
                }
            } else {
                for (IteratorScope scope : IteratorScope.values()) {
                    if (connector.tableOperations().getIteratorSetting(metricsTable, "ageoff", scope) == null) {
                        connector.tableOperations().attachIterator(metricsTable, ageOffIteratorSettings,
                                EnumSet.of(scope));
                    }
                }
            }
            metaTable = conf.get(Configuration.META_TABLE);
            if (!tableIdMap.containsKey(metaTable)) {
                try {
                    LOG.info("Creating table " + metaTable);
                    connector.tableOperations().create(metaTable);
                    connector.tableOperations().attachIterator(metaTable, ageOffIteratorSettings, ageOffIteratorScope);
                } catch (final TableExistsException ex) {
                    // don't care
                }
            } else {
                for (IteratorScope scope : IteratorScope.values()) {
                    if (connector.tableOperations().getIteratorSetting(metaTable, "ageoff", scope) == null) {
                        connector.tableOperations()
                                .attachIterator(metaTable, ageOffIteratorSettings, EnumSet.of(scope));
                    }
                }
            }
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
    public void store(Metric metric) {
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
            Meta key = new Meta(metric.getMetric(), tag.getKey(), tag.getValue());
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
            batchWriter.get().addMutation(metric.toMutation());
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

    private void updateMetricCounts() {
        long now = System.currentTimeMillis();
        if (now - lastCountTime.get() > 5 * 60 * 1000) {
            this.lastCountTime.set(now);
            SortedMap<MetricTagK, Integer> update = new TreeMap<>();
            for (Meta meta : this.metaCache) {
                MetricTagK key = new MetricTagK(meta.getMetric(), meta.getTagKey());
                Integer count = update.getOrDefault(key, Integer.valueOf(0));
                update.put(key, Integer.valueOf(count.intValue() + 1));
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
        try {
            List<Result> resultField = new ArrayList<>();
            Scanner scanner = connector.createScanner(metaTable, Authorizations.EMPTY);
            Key start = new Key(Meta.VALUE_PREFIX + msg.getQuery());
            Key end = start.followingKey(PartialKey.ROW);
            Range range = new Range(start, end);
            scanner.setRange(range);
            // TODO compute columns to fetch
            int total = 0;
            for (Entry<Key, Value> entry : scanner) {
                Meta metaEntry = Meta.parse(entry.getKey(), entry.getValue());
                if (matches(metaEntry.getTagKey(), metaEntry.getTagValue(), tags)) {
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

    private boolean matches(String tagk, String tagv, Map<String, String> tags) {
        // first, match keys...
        for (Entry<String, String> entry : tags.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            if (("*".equals(k) || tagk.equals(k)) && ("*".equals(v) || tagv.equals(v))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<QueryResponse> query(QueryRequest msg) throws TimelyException {
        List<QueryResponse> result = new ArrayList<>();
        long startTs = msg.getStart();
        long endTs = msg.getEnd();
        try {
            long now = System.currentTimeMillis();
            for (SubQuery query : msg.getQueries()) {
                Map<Set<Tag>, List<Downsample>> allSeries = new HashMap<>();
                String metric = query.getMetric();
                BatchScanner scanner = connector.createBatchScanner(metricsTable, getSessionAuthorizations(msg),
                        scannerThreads);
                try {
                    setQueryRange(scanner, metric, startTs, endTs);
                    setQueryColumns(scanner, metric, query.getTags());
                    long downsample = getDownsamplePeriod(query);
                    LOG.trace("Downsample period {}", downsample);
                    Class<? extends Aggregator> aggClass = getAggregator(query);
                    LOG.trace("Aggregator type {}", aggClass.getSimpleName());
                    IteratorSetting is = new IteratorSetting(500, DownsampleIterator.class);
                    DownsampleIterator.setDownsampleOptions(is, startTs, endTs, downsample, prioritizeTags(query),
                            aggClass.getName());
                    scanner.addScanIterator(is);
                    // tag -> array of results by period starting at start
                    for (Entry<Key, Value> encoded : scanner) {
                        Map<Set<Tag>, Downsample> samples = DownsampleIterator.decodeValue(encoded.getValue());
                        for (Entry<Set<Tag>, Downsample> entry : samples.entrySet()) {
                            Set<Tag> key = new HashSet<>();
                            for (Tag tag : entry.getKey()) {
                                if (query.getTags().keySet().contains(tag.getKey())) {
                                    key.add(tag);
                                }
                            }
                            List<Downsample> downsamples = allSeries.getOrDefault(key, new ArrayList<>());
                            downsamples.add(entry.getValue());
                            allSeries.put(key, downsamples);
                        }
                    }
                    LOG.trace("allSeries: {}", allSeries);
                } finally {
                    scanner.close();
                }

                // TODO groupby here?
                long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                for (Entry<Set<Tag>, List<Downsample>> entry : allSeries.entrySet()) {
                    result.add(convertToQueryResponse(query, entry.getKey(), entry.getValue(), tsDivisor));
                }
            }
            LOG.debug("Query time: {}", (System.currentTimeMillis() - now));
            return result;
        } catch (ClassNotFoundException | IOException | TableNotFoundException ex) {
            LOG.error("Error during query: " + ex.getMessage(), ex);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + ex.getMessage(), ex.getMessage(), ex);
        }
    }

    private List<String> prioritizeTags(SubQuery query) {
        // trivial cases
        Map<String, String> tags = query.getTags();
        if (tags.isEmpty()) {
            return Collections.emptyList();
        }
        if (tags.size() == 1) {
            return Collections.singletonList(tags.keySet().iterator().next());
        }
        // favor tags with fewer values
        Map<String, Integer> priority = new HashMap<>();
        String metric = query.getMetric();
        // Count matching tags
        updateMetricCounts();
        for (Entry<String, String> entry : tags.entrySet()) {
            String tagk = entry.getKey();
            String tagv = entry.getValue();
            int wildcard = tagv.lastIndexOf('*');
            if (wildcard < 0) {
                // no wildcard: can only match one tag
                priority.put(tagk, 1);
                continue;
            }
            String prefix = tagv.substring(0, wildcard);
            int count = 0;
            MetricTagK start = new MetricTagK(metric, prefix);
            for (Entry<MetricTagK, Integer> metricCount : metaCounts.get().tailMap(start).entrySet()) {
                Pair<String, String> metricTagk = metricCount.getKey();
                if (!metricTagk.getFirst().equals(metric) || !metricTagk.getSecond().startsWith(prefix)) {
                    break;
                }
            }
            priority.put(tagk, count);
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

    private QueryResponse convertToQueryResponse(SubQuery query, Set<Tag> tags, Collection<Downsample> values,
            long tsDivisor) {
        QueryResponse response = new QueryResponse();
        response.setMetric(query.getMetric());
        for (Tag tag : tags) {
            response.putTag(tag.getKey(), tag.getValue());
        }
        RateOption rateOptions = query.getRateOptions();
        Downsample combined = Downsample.combine(values, rateOptions);
        for (Sample entry : combined) {
            long ts = entry.timestamp / tsDivisor;
            response.putDps(Long.toString(ts), entry.value);
        }
        LOG.trace("Created query response {}", response);
        return response;
    }

    private void setQueryColumns(BatchScanner scanner, String metric, Map<String, String> tags)
            throws TableNotFoundException, TimelyException {
        LOG.trace("Looking for requested tags: {}", tags);
        Scanner meta = connector.createScanner(metaTable, Authorizations.EMPTY);
        Text start = new Text(Meta.VALUE_PREFIX + metric);
        Text end = new Text(Meta.VALUE_PREFIX + metric + "\\x0000");
        end.append(new byte[] { (byte) 0xff }, 0, 1);
        meta.setRange(new Range(start, end));
        // Only look for the meta entries that match our tags, if any
        boolean onlyFirstRow = false;
        Entry<String, String> first = null;
        Iterator<Entry<String, String>> tagIter = tags.entrySet().iterator();
        if (tagIter.hasNext()) {
            first = tagIter.next();
            if (first.getValue().endsWith("*")) {
                meta.fetchColumnFamily(new Text(first.getKey()));
            } else {
                meta.fetchColumn(new Text(first.getKey()), new Text(first.getValue()));
            }
        } else {
            // only grab the first meta entry for this metric
            onlyFirstRow = true;
        }
        final boolean ONLY_RETURN_FIRST_ROW = onlyFirstRow;
        Iterator<Entry<Key, Value>> iter = meta.iterator();
        Iterator<Pair<String, String>> knownKeyValues = new Iterator<Pair<String, String>>() {

            boolean returnedFirstRecord = false;

            @Override
            public boolean hasNext() {
                if (ONLY_RETURN_FIRST_ROW) {
                    if (this.returnedFirstRecord) {
                        return false;
                    } else {
                        return iter.hasNext();
                    }
                } else {
                    return iter.hasNext();
                }
            }

            @Override
            public Pair<String, String> next() {
                Entry<Key, Value> entry = iter.next();
                this.returnedFirstRecord = true;
                LOG.trace("Returning tag {}={}", entry.getKey().getColumnFamily().toString(), entry.getKey()
                        .getColumnQualifier().toString());
                return new Pair<>(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier()
                        .toString());
            }
        };
        Set<Tag> concrete = expandTagValues(first, knownKeyValues);
        if (concrete.size() == 0) {
            throw new TimelyException(HttpResponseStatus.BAD_REQUEST.code(), "No matching tags", "No tags were found "
                    + " that matched the submitted tags. Please fix and retry");
        }
        LOG.trace("Found matching tags: {}", concrete);
        for (Tag tag : concrete) {
            Text colf = new Text(tag.getKey() + "=" + tag.getValue());
            scanner.fetchColumnFamily(colf);
            LOG.trace("Fetching column family: {}", colf);
        }
        // Add the regular expression to filter the other tags
        int priority = 100;
        while (tagIter.hasNext()) {
            Entry<String, String> tag = tagIter.next();
            LOG.trace("Additional tag {}", tag);
            StringBuffer pattern = new StringBuffer();
            pattern.append("(^|,)").append(Pattern.quote(tag.getKey())).append("=");
            String value = tag.getValue();
            if (tag.getValue().endsWith("*")) {
                value = value.substring(0, value.length() - 1);
                pattern.append(Pattern.quote(value));
                pattern.append("[^,]*");
            } else {
                pattern.append(Pattern.quote(value));
                pattern.append("(,|$)");
            }
            IteratorSetting setting = new IteratorSetting(priority++, tag.getKey() + " tag filter", RegExFilter.class);
            LOG.trace("Using {} additional filter on tags", pattern);
            RegExFilter.setRegexs(setting, null, null, pattern.toString(), null, false, true);
            scanner.addScanIterator(setting);
        }
    }

    private Set<Tag> expandTagValues(Entry<String, String> firstTag, Iterator<Pair<String, String>> knownKeyValues) {
        Set<Tag> result = new HashSet<>();
        while (knownKeyValues.hasNext()) {
            Pair<String, String> knownKeyValue = knownKeyValues.next();
            if (firstTag == null) {
                LOG.trace("Adding tag {}={}", knownKeyValue.getFirst(), knownKeyValue.getSecond());
                result.add(new Tag(knownKeyValue.getFirst(), knownKeyValue.getSecond()));
            } else {
                LOG.trace("Testing requested tag {}={}", firstTag.getKey(), firstTag.getValue());
                if (firstTag.getKey().equals(knownKeyValue.getFirst())) {
                    if (firstTag.getValue().endsWith("*")) {
                        String prefix = firstTag.getValue().substring(0, firstTag.getValue().length() - 1);
                        if (knownKeyValue.getSecond().startsWith(prefix)) {
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

    private void setQueryRange(BatchScanner scanner, String metric, long start, long end) {
        final byte[] start_row = Metric.encodeRowKey(metric, start);
        LOG.trace("Start key for metric {} and time {} is {}", metric, start, start_row);
        final byte[] end_row = Metric.encodeRowKey(metric, end);
        LOG.trace("End key for metric {} and time {} is {}", metric, end, end_row);
        Range range = new Range(new Text(start_row), new Text(end_row));
        LOG.trace("Set query range to {}", range);
        scanner.setRanges(Collections.singletonList(range));
    }

    private Class<? extends Aggregator> getAggregator(SubQuery query) {
        String aggregatorName = "avg";
        if (query.getDownsample().isPresent()) {
            String parts[] = query.getDownsample().get().split("-");
            aggregatorName = parts[1];
        }
        return Aggregator.getAggregator(aggregatorName);
    }

    private long getDownsamplePeriod(SubQuery query) {
        if (!query.getDownsample().isPresent()) {
            return DEFAULT_DOWNSAMPLE_MS;
        }
        String parts[] = query.getDownsample().get().split("-");
        return getTimeInMillis(parts[0]);
    }

    private Authorizations getSessionAuthorizations(AuthenticatedRequest request) {
        String sessionId = request.getSessionId();
        if (!StringUtils.isEmpty(sessionId)) {
            return AuthCache.getAuthorizations(sessionId);
        } else if (!anonAccessAllowed) {
            throw new RuntimeException("Anonymous user attempting to query");
        } else {
            return Authorizations.EMPTY;
        }

    }

}
