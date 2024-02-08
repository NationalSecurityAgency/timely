package timely.store.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TieredCompactionStrategy extends CompactionStrategy {

    public static final String TIERED_PREFIX = "tier.";
    public static final String TIERED_CLASS = "class";
    public static final String TIERED_OPTS = "opts";

    protected final Collection<TieredCompactorSupplier> compactors;

    public TieredCompactionStrategy() {
        compactors = new ArrayList<>();
    }

    public Collection<TieredCompactorSupplier> getCompactors() {
        return Collections.unmodifiableCollection(compactors);
    }

    @Override
    public void init(Map<String,String> config) {
        // example:
        // <table-compaction-prefix>.tiered.0.class=<majcompaction-class>
        // <table-compaction-prefix>.tiered.0.opts.key=value
        // <table-compaction-prefix>.tiered.1.class=<majcompaction-class>
        // <table-compaction-prefix>.tiered.1.opts.key_a=value
        // <table-compaction-prefix>.tiered.1.opts.key_b=value
        //
        // the key=value pairs will be sent to the specific tiered compaction
        // strategy as arguments

        compactors.clear();
        createSuppliers(config);
    }

    @Override
    public void gatherInformation(MajorCompactionRequest request) throws IOException {
        for (TieredCompactorSupplier compactor : compactors) {
            CompactionStrategy strategy = compactor.get(request.getTableProperties());
            strategy.gatherInformation(request);
        }
    }

    @Override
    public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
        boolean shouldCompact = false;
        for (TieredCompactorSupplier supplier : compactors) {
            CompactionStrategy strategy = supplier.get(request.getTableProperties());
            shouldCompact = strategy.shouldCompact(request);
            if (shouldCompact) {
                break;
            }
        }

        return shouldCompact;
    }

    @Override
    public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
        CompactionPlan plan = null;
        Iterator<TieredCompactorSupplier> iter = compactors.iterator();
        while ((null == plan || plan.inputFiles.isEmpty()) && iter.hasNext()) {
            CompactionStrategy strategy = iter.next().get(request.getTableProperties());
            plan = strategy.getCompactionPlan(request);
        }

        return plan;
    }

    protected void createSuppliers(Map<String,String> config) {
        int tier = 0;
        boolean continueLoop = true;
        while (continueLoop) {
            String clazzKey = TIERED_PREFIX + tier + "." + TIERED_CLASS;
            String clazzName = config.get(clazzKey);
            if (null != clazzName) {
                compactors.add(new TieredConfiguredSupplier(clazzName, tier, config));
                tier++;
            } else {
                continueLoop = false;
            }
        }
    }

    static class TieredConfiguredSupplier implements TieredCompactorSupplier {

        private static final Logger LOG = LoggerFactory.getLogger(TieredConfiguredSupplier.class);

        private final String clazzName;
        private final int tier;
        private final Map<String,String> initConfig;

        private CompactionStrategy strategy;
        private Map<String,String> options;

        TieredConfiguredSupplier(String clazzName, int tier, Map<String,String> initConfig) {
            this.strategy = null;
            this.clazzName = clazzName;
            this.tier = tier;
            this.initConfig = initConfig;
        }

        public Map<String,String> options() {
            if (null == options) {
                options = new HashMap<>();
                String tierCompare = Integer.toString(tier);
                int tierIdx = TIERED_PREFIX.length();
                int optsIdx = TIERED_PREFIX.length() + tierCompare.length() + 1;
                for (Map.Entry<String,String> entry : initConfig.entrySet()) {
                    String key = entry.getKey();
                    if (key.regionMatches(tierIdx, tierCompare, 0, tierCompare.length()) && key.regionMatches(optsIdx, TIERED_OPTS, 0, TIERED_OPTS.length())) {
                        options.put(key.substring(optsIdx + TIERED_OPTS.length() + 1), entry.getValue());
                    }
                }
            }

            return Collections.unmodifiableMap(options);
        }

        public CompactionStrategy get(Map<String,String> tableProperties) {
            if (null == strategy) {
                try {
                    String tableContext = tableProperties.get(Property.TABLE_CLASSPATH.getKey());
                    Class<? extends CompactionStrategy> clazz;
                    if (null != tableContext && !tableContext.equals("")) {
                        clazz = AccumuloVFSClassLoader.getContextClassLoader(tableContext).loadClass(clazzName).asSubclass(CompactionStrategy.class);
                    } else {
                        clazz = AccumuloVFSClassLoader.getClassLoader().loadClass(clazzName).asSubclass(CompactionStrategy.class);
                    }
                    strategy = clazz.newInstance();
                    strategy.init(options());
                } catch (Exception e) {
                    LOG.debug("Failed to load class for tiered compaction: " + e.toString());
                    throw new RuntimeException(e);
                }
            }

            return strategy;
        }
    }
}
