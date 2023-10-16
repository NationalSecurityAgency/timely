package timely.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.store.MetricAgeOffIterator;
import timely.store.compaction.MetricCompactionStrategy;
import timely.store.compaction.TabletRowAdapter;
import timely.store.compaction.util.TabletMetadataQuery;
import timely.store.compaction.util.TabletMetadataView;
import timely.store.compaction.util.TabletStatistic;
import timely.store.compaction.util.TabletSummary;
import timely.test.IntegrationTest;
import timely.test.TestMetricWriter;
import timely.test.TestMetrics;

@Category(IntegrationTest.class)
public class MetricCompactionIT extends MacITBase {

    private final static Logger LOG = LoggerFactory.getLogger(MetricCompactionIT.class);

    private static final String AGE_OFF_ITERATOR_NAME = "ageoffmetrics";
    private static final EnumSet<IteratorUtil.IteratorScope> AGEOFF_SCOPES = EnumSet
            .allOf(IteratorUtil.IteratorScope.class);

    private AccumuloClient accumuloClient;
    private MetricConfigurationAdapter adapter;

    @BeforeClass
    public static void setupClass() throws Exception {
        try (AccumuloClient accumuloClient = mac.createAccumuloClient(MAC_ROOT_USER,
                new PasswordToken(MAC_ROOT_PASSWORD))) {
            accumuloClient.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "3s");
        }
    }

    @Before
    public void setup() throws Exception {
        accumuloClient = mac.createAccumuloClient(MAC_ROOT_USER, new PasswordToken(MAC_ROOT_PASSWORD));
        String metricsTable = conf.getMetricsTable();
        if (!metricsTable.contains(".")) {
            throw new IllegalArgumentException("Expected to find namespace in table name");
        }
        String[] parts = metricsTable.split("\\.", 2);
        String namespace = parts[0];
        if (!accumuloClient.namespaceOperations().exists(namespace)) {
            accumuloClient.namespaceOperations().create(namespace);
        }
        accumuloClient.tableOperations().create(metricsTable);
        adapter = new MetricConfigurationAdapter(accumuloClient, conf.getMetricsTable());
        accumuloClient.tableOperations().setProperty(conf.getMetricsTable(), Property.TABLE_SPLIT_THRESHOLD.getKey(),
                "50K");
        adapter.resetState();
    }

    @After
    public void teardown() throws Exception {
        adapter.resetState();
        if (accumuloClient != null) {
            accumuloClient.close();
        }
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        try (AccumuloClient accumuloClient = mac.createAccumuloClient(MAC_ROOT_USER,
                new PasswordToken(MAC_ROOT_PASSWORD))) {
            accumuloClient.instanceOperations().setProperty(Property.TSERV_MAJC_DELAY.getKey(), "30s");
        }
    }

    @Test
    public void tableMetadataEnumeratesTablets() throws Exception {
        String metricsTable = conf.getMetricsTable();

        TestMetricWriter writer = new TestMetricWriter(metricsTable, accumuloClient, new TestMetrics());
        writer.ingestRandomDuration(60, TimeUnit.SECONDS, System.currentTimeMillis(), 250);

        Collection<Text> splits = accumuloClient.tableOperations().listSplits(conf.getMetricsTable());
        TabletMetadataQuery query = new TabletMetadataQuery(accumuloClient, metricsTable);
        TabletMetadataView view = query.run();

        // @formatter:off
        TabletSummary summary = view.computeSummary()
                .disableTabletRowCheckFilter()
                .build();

        Set<String> splitsPrefixed = splits.stream()
                .map(row -> TabletRowAdapter.decodeRowPrefix(row).orElse("unknown"))
                .collect(Collectors.toSet());

        Set<String> tabletsPrefixed = summary.getSummary().stream()
                .map(TabletStatistic::getKeyName)
                .collect(Collectors.toSet());

        // difference both sides
        // should be empty
        // @formatter:on
        Set<String> i1 = Sets.difference(tabletsPrefixed, splitsPrefixed);
        Set<String> i2 = Sets.difference(splitsPrefixed, tabletsPrefixed);
        assertTrue("tablets-diff" + i1.toString(), i1.isEmpty());
        assertTrue("splits-diff" + i2.toString(), i2.isEmpty());

        // dump data to screen as summary
        System.out.println(view.toText());
    }

    @Test
    public void tabletsCompactedUntilAgeOff() throws Exception {
        // test will replicated problem in time-series tablets where data is
        // split automatically and the resulting tablets will not be cleaned up
        // by the default compaction strategy
        //
        // metric compaction strategy will age-off tablets by date on tablets
        // but ignore differing end/prev rows
        //
        // the mini-accumulo cluster should not have any age-off
        // iterators on timely.metrics

        String metricsTable = conf.getMetricsTable();

        // check to make sure the table has only one iterator
        Map<String, EnumSet<IteratorUtil.IteratorScope>> itrs = accumuloClient.tableOperations()
                .listIterators(metricsTable);
        assertEquals(1, itrs.size());
        assertTrue(itrs.containsKey("vers"));

        long timestampMax = System.currentTimeMillis();
        TestMetricWriter writer = new TestMetricWriter(conf.getMetricsTable(), accumuloClient, new TestMetrics());
        long timestampMin = writer.ingestRandomDuration(30, TimeUnit.SECONDS, timestampMax, 250);

        // invoke compact operation to flush everything through
        // without this call the compaction strategy later on
        // was not compacting all of the tablets
        adapter.runCompaction();

        // figure out what should be targeted based on an ageoff date
        // cut the delta by 1/3
        long ageoff = (timestampMax - timestampMin) / 3;

        // run check to verify there are pre-existing splits for a prefix
        // @formatter:off
        TabletMetadataQuery query = new TabletMetadataQuery(accumuloClient, metricsTable);
        TabletSummary summary1 = query.run()
                .computeSummary()
                .build();

        Set<String> tabletsBefore = summary1.getSummary().stream()
                .filter(t -> t.maxAge() > ageoff)
                .map(TabletStatistic::getKeyName)
                .collect(Collectors.toSet());

        // @formatter:on
        assertFalse("Tablets were empty before compaction, expected test data", tabletsBefore.isEmpty());

        long compactBeforeMillis = System.currentTimeMillis();

        // invoke the age-off compactor
        // after this runs it should have gotten rid o fdata
        // have pre-age-off tablets
        adapter.applyCompactionConfiguration(MetricCompactionStrategy.class, ageoff);
        adapter.runCompaction();

        long compactDeltaMillis = System.currentTimeMillis() - compactBeforeMillis;

        // re-run check on tablet metadata
        // check that there aren't new tablets past the predicted age-off
        // any tablets with differing end/prev rows are filtered by the summary
        // @formatter:off
        TabletSummary summary2 = query.run()
                .computeSummary()
                .build();

        List<TabletStatistic> tabletsFilterAfter = summary2.getSummary().stream()
                .filter(t -> ((t.maxAge() - compactDeltaMillis) > ageoff) && t.getKeyCount() > 1)
                .collect(Collectors.toList());

        // @formatter:on
        if (!tabletsFilterAfter.isEmpty()) {
            LOG.info("tablets-before: {}", tabletsBefore.toString());
            LOG.info("tablets-after: {}", tabletsFilterAfter.toString());
        }

        // should not have found tablets past the age-off and should also
        // have found existing tablets still in system (i.e. did not compact everything)
        assertTrue("Tablets still had unexpected after compaction", tabletsFilterAfter.isEmpty());
        assertTrue("Some tablets should exist, but did not find any", summary2.totalTablets() > 0);
    }

    private static class MetricConfigurationAdapter {

        private final AccumuloClient accumuloClient;
        private final String tableName;

        private static String[] CLEAR_PROPERTY_KEYS = {
                Property.TABLE_COMPACTION_STRATEGY_PREFIX.getKey() + MetricCompactionStrategy.MIN_AGEOFF_KEY };

        public MetricConfigurationAdapter(AccumuloClient accumuloClient, String tableName) {
            this.accumuloClient = accumuloClient;
            this.tableName = tableName;
        }

        public void applyCompactionConfiguration(Class<? extends CompactionStrategy> clazz, long ageOff)
                throws Exception {
            Map<String, String> ageOffs = new HashMap<>();
            ageOffs.put("ageoff.default", Long.toString(ageOff));
            IteratorSetting ageOffIteratorSettings = new IteratorSetting(100, AGE_OFF_ITERATOR_NAME,
                    MetricAgeOffIterator.class, ageOffs);
            accumuloClient.tableOperations().attachIterator(tableName, ageOffIteratorSettings, AGEOFF_SCOPES);
            accumuloClient.tableOperations().setProperty(tableName, Property.TABLE_COMPACTION_STRATEGY.getKey(),
                    clazz.getName());
        }

        public void runCompaction() throws Exception {
            accumuloClient.tableOperations().compact(tableName, null, null, true, true);
        }

        public void resetState() throws Exception {
            Map<String, EnumSet<IteratorUtil.IteratorScope>> iters = accumuloClient.tableOperations()
                    .listIterators(tableName);
            for (String name : iters.keySet()) {
                if (name.startsWith("ageoff")) {
                    accumuloClient.tableOperations().removeIterator(tableName, name, AGEOFF_SCOPES);
                }
            }

            // reset compaction strategy to default
            accumuloClient.tableOperations().setProperty(tableName, Property.TABLE_COMPACTION_STRATEGY.getKey(),
                    DefaultCompactionStrategy.class.getName());

            // reset any properties
            for (String s : CLEAR_PROPERTY_KEYS) {
                accumuloClient.tableOperations().removeProperty(tableName, s);
            }
        }
    }
}
