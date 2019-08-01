package timely.store.compaction;

import java.util.Map;

import org.apache.accumulo.tserver.compaction.CompactionStrategy;

public interface TieredCompactorSupplier {

    Map<String, String> options();

    CompactionStrategy get(Map<String, String> tableProperties);
}
