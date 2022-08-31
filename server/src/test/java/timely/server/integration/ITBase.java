package timely.server.integration;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import timely.common.configuration.ServerProperties;
import timely.common.configuration.TimelyProperties;
import timely.server.configuration.TabletMetadataProperties;
import timely.server.test.TestDataStore;
import timely.server.test.TestDataStoreCache;

/**
 * Base class for integration tests using in-memory-accumulo.
 */

public class ITBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    protected ServerProperties serverProperties;

    @Autowired
    protected TestDataStore dataStore;

    @Autowired
    protected TestDataStoreCache dataStoreCache;

    @Autowired
    protected AccumuloClient accumuloClient;

    @Autowired
    protected TimelyProperties timelyProperties;

    @Autowired
    protected TabletMetadataProperties tabletMetadataProperties;

    private Authorizations authorizations;

    @Before
    public void setup() {
        try {
            try {
                authorizations = this.accumuloClient.securityOperations().getUserAuthorizations(this.accumuloClient.whoami());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            String metricsTable = timelyProperties.getMetricsTable();
            if (!metricsTable.contains(".")) {
                throw new IllegalArgumentException("Expected to find namespace in table name");
            }
            String[] parts = metricsTable.split("\\.", 2);
            String namespace = parts[0];
            if (!accumuloClient.namespaceOperations().exists(namespace)) {
                accumuloClient.namespaceOperations().create(namespace);
            }

            if (!accumuloClient.tableOperations().exists(timelyProperties.getMetricsTable())) {
                accumuloClient.tableOperations().create(timelyProperties.getMetricsTable());
            }
            if (!accumuloClient.tableOperations().exists(timelyProperties.getMetaTable())) {
                accumuloClient.tableOperations().create(timelyProperties.getMetaTable());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        dataStore.reset();

        // some tests remove the version iterator, so we make sure that it is applied here
        IteratorSetting versionItr = new IteratorSetting(20, "vers", VersioningIterator.class);
        try {
            accumuloClient.tableOperations().attachIterator(timelyProperties.getMetricsTable(), versionItr, EnumSet.of(IteratorUtil.IteratorScope.scan));
        } catch (AccumuloException e) {
            // Will get an iterator priority conflict if already applied
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        try {
            accumuloClient.tableOperations().attachIterator(timelyProperties.getMetaTable(), versionItr, EnumSet.of(IteratorUtil.IteratorScope.scan));
        } catch (AccumuloException e) {
            // Will get an iterator priority conflict if already applied
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @After
    public void cleanup() {
        try {
            accumuloClient.tableOperations().clearLocatorCache(timelyProperties.getMetricsTable());
            accumuloClient.tableOperations().clearLocatorCache(timelyProperties.getMetaTable());
            deleteTable(timelyProperties.getMetricsTable());
            deleteTable(timelyProperties.getMetaTable());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected void deleteEntries(String tableName) {
        try {
            accumuloClient.tableOperations().deleteRows(tableName, new Text("#"), new Text("~"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void deleteTable(String tableName) {
        try {
            accumuloClient.tableOperations().delete(tableName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected void printAllAccumuloEntries() {
        getAllAccumuloEntries().forEach(s -> System.out.println(s));
    }

    protected Collection<String> getAllAccumuloEntries() {
        List<String> entries = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        tables.add(timelyProperties.getMetricsTable());
        tables.add(timelyProperties.getMetaTable());
        tables.forEach(t -> {
            entries.addAll(getAccumuloEntryStrings(t));
        });
        return entries;
    }

    protected Collection<String> getAccumuloEntryStrings(String table) {
        List<String> entryStrings = new ArrayList<>();
        try {
            Collection<Map.Entry<Key,Value>> entries = getAccumuloEntries(this.accumuloClient, table);
            for (Map.Entry<Key,Value> e : entries) {
                entryStrings.add(table + " -> " + e.getKey());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entryStrings;
    }

    public Collection<Map.Entry<Key,Value>> getAccumuloEntries(AccumuloClient accumuloClient, String table) throws Exception {
        Collection<Map.Entry<Key,Value>> entries = new ArrayList<>();
        try (BatchScanner bs = accumuloClient.createBatchScanner(table, authorizations, 1)) {
            bs.setRanges(Collections.singletonList(new Range()));
            final Iterator<Map.Entry<Key,Value>> itr = bs.iterator();
            while (itr.hasNext()) {
                entries.add(itr.next());
            }
        }
        return entries;
    }

    protected void put(String... lines) throws Exception {
        final CountDownLatch PUT_REQUESTS = new CountDownLatch(lines.length);
        TestDataStore.StoreCallback storeCallback = () -> PUT_REQUESTS.countDown();
        try {
            dataStore.addStoreCallback(storeCallback);
            StringBuffer format = new StringBuffer();
            for (String line : lines) {
                format.append("put ");
                format.append(line);
                format.append("\n");
            }
            try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                            PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
                writer.write(format.toString());
                writer.flush();
            }
            PUT_REQUESTS.await(5, TimeUnit.SECONDS);
        } finally {
            dataStore.removeStoreCallback(storeCallback);
        }
    }
}
