package timely.store.compaction.util;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;

public class TabletMetadataQuery {

    private final Connector connector;
    private final String tableName;

    public TabletMetadataQuery(Connector connector, String tableName) {
        this.connector = connector;
        this.tableName = tableName;
    }

    public TabletMetadataView run() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        TabletMetadataView view = new TabletMetadataView();
        Map<String, String> tableMap = connector.tableOperations().tableIdMap();
        String tableId = tableMap.get(tableName);
        if (null == tableId) {
            throw new IllegalStateException("Unable to find " + MetadataTable.NAME);
        }

        try (Scanner s = connector.createScanner(MetadataTable.NAME,
                connector.securityOperations().getUserAuthorizations(connector.whoami()))) {
            // s.setRange(new Range(tableId, true, tableId, true));
            s.setRange(Range.prefix(tableId));
            for (Map.Entry<Key, Value> e : s) {
                view.addEntry(e);
            }
        }

        return view;
    }
}
