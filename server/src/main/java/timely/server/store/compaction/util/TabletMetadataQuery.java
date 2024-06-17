package timely.server.store.compaction.util;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.AccumuloTable;

import timely.common.configuration.TimelyProperties;
import timely.server.configuration.TabletMetadataProperties;

public class TabletMetadataQuery {

    private AccumuloClient accumuloClient;
    private TimelyProperties timelyProperties;
    private TabletMetadataProperties tabletMetadataProperties;

    public TabletMetadataQuery(AccumuloClient accumuloClient, TimelyProperties timelyProperties, TabletMetadataProperties TabletMetadataProperties) {
        this.accumuloClient = accumuloClient;
        this.timelyProperties = timelyProperties;
        this.tabletMetadataProperties = TabletMetadataProperties;
    }

    public TabletMetadataView run() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        TabletMetadataView tabletMetadataView = new TabletMetadataView(tabletMetadataProperties);
        Map<String,String> tableMap = accumuloClient.tableOperations().tableIdMap();
        String tableId = tableMap.get(timelyProperties.getMetricsTable());
        if (null == tableId) {
            throw new IllegalStateException("Unable to find " + AccumuloTable.METADATA.tableName());
        }

        try (Scanner s = accumuloClient.createScanner(AccumuloTable.METADATA.tableName(),
                        accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()))) {
            s.setRange(Range.prefix(tableId));
            for (Map.Entry<Key,Value> e : s) {
                tabletMetadataView.addEntry(e);
            }
        }

        return tabletMetadataView;
    }
}
