package timely.util;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    public static void deleteAndCreateTables(AccumuloClient accumuloClient, List<String> tables) {
        try {
            tables.forEach(t -> {
                try {
                    accumuloClient.tableOperations().delete(t);
                    while (accumuloClient.tableOperations().exists(t)) {
                        log.info("Waiting for table " + t + " to be deleted");
                        Thread.sleep(200);
                    }
                    accumuloClient.tableOperations().create(t);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void printTable(AccumuloClient accumuloClient, String table, boolean useFormatter) {

        try {
            try (Scanner scanner = accumuloClient.createScanner(table, Authorizations.EMPTY)) {
                scanner.setRange(new Range());
                for (Map.Entry<Key,Value> e : scanner) {
                    if (useFormatter) {
                        log.info("Entry {}", TimelyMetricsFormatter.formatMetricEntry(e, true));
                    } else {
                        log.info("Entry key={} value={}", e.getKey(), e.getValue());
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
