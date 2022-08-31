package timely.server.store.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.Lists;

import timely.server.test.CompactionRequestBuilder;

public class TieredCompactionStrategyTest {

    @Test
    public void initConfiguresWithMultipleTiers() {
        Map<String,String> config = new HashMap<>();
        config.put("tier.0.class", DefaultCompactionStrategy.class.getName());
        config.put("tier.0.opts.arg1", "foo");
        config.put("tier.0.opts.arg2", "sys");
        config.put("tier.1.class", DefaultCompactionStrategy.class.getName());
        config.put("tier.1.opts.arg1", "bar");
        config.put("tier.2.class", DefaultCompactionStrategy.class.getName());

        TieredCompactionStrategy strategy = new TieredCompactionStrategy();
        strategy.init(config);
        List<TieredCompactorSupplier> suppliers = Lists.newArrayList(strategy.getCompactors());
        assertEquals(3, suppliers.size());
        assertEquals(2, suppliers.get(0).options().size());
        assertEquals(1, suppliers.get(1).options().size());
        assertEquals(0, suppliers.get(2).options().size());
        assertEquals("foo", suppliers.get(0).options().get("arg1"));
        assertEquals("sys", suppliers.get(0).options().get("arg2"));
        assertEquals("bar", suppliers.get(1).options().get("arg1"));
    }

    @Test
    public void initConfiguresWithSingleTierNoArguments() {
        Map<String,String> config = new HashMap<>();
        config.put("tier.0.class", DefaultCompactionStrategy.class.getName());

        TieredCompactionStrategy strategy = new TieredCompactionStrategy();
        strategy.init(config);
        List<TieredCompactorSupplier> suppliers = Lists.newArrayList(strategy.getCompactors());
        assertEquals(1, suppliers.size());
        assertEquals(0, suppliers.get(0).options().size());
    }

    @Test
    public void gatherInformationAcrossTiers() throws IOException {
        CompactionStrategy c1 = EasyMock.createMock(CompactionStrategy.class);
        CompactionStrategy c2 = EasyMock.createMock(CompactionStrategy.class);

        c1.gatherInformation(EasyMock.anyObject());
        EasyMock.expectLastCall();

        c2.gatherInformation(EasyMock.anyObject());
        EasyMock.expectLastCall();

        EasyMock.replay(c1, c2);

        TieredCompactionStrategy strategy = newStrategy(c1, c2);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        strategy.gatherInformation(request);

        EasyMock.verify(c1, c2);
    }

    @Test
    public void shouldCompactStopsAtTier() throws IOException {
        CompactionStrategy c1 = EasyMock.createMock("c1", CompactionStrategy.class);
        CompactionStrategy c2 = EasyMock.createMock("c2", CompactionStrategy.class);
        CompactionStrategy c3 = EasyMock.createMock("c3", CompactionStrategy.class);

        EasyMock.expect(c1.shouldCompact(EasyMock.anyObject())).andReturn(false);
        EasyMock.expect(c2.shouldCompact(EasyMock.anyObject())).andReturn(true);

        EasyMock.replay(c1, c2, c3);

        TieredCompactionStrategy strategy = newStrategy(c1, c2, c3);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        boolean result = strategy.shouldCompact(request);

        assertTrue(result);
        EasyMock.verify(c1, c2, c3);
    }

    @Test
    public void shouldCompactWithSingleTier() throws Exception {
        CompactionStrategy c1 = EasyMock.createMock("c1", CompactionStrategy.class);

        EasyMock.expect(c1.shouldCompact(EasyMock.anyObject())).andReturn(false);
        EasyMock.replay(c1);

        TieredCompactionStrategy strategy = newStrategy(c1);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        boolean result = strategy.shouldCompact(request);

        assertFalse(result);
        EasyMock.verify(c1);
    }

    @Test
    public void getCompactionPlanSkipsAndStopsAtTier() throws IOException {
        CompactionPlan p1 = new CompactionPlan();
        p1.inputFiles.add(new StoredTabletFile("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf"));

        CompactionStrategy c1 = EasyMock.createMock("c1", CompactionStrategy.class);
        CompactionStrategy c2 = EasyMock.createMock("c2", CompactionStrategy.class);
        CompactionStrategy c3 = EasyMock.createMock("c3", CompactionStrategy.class);

        EasyMock.expect(c1.getCompactionPlan(EasyMock.anyObject())).andReturn(null);
        EasyMock.expect(c2.getCompactionPlan(EasyMock.anyObject())).andReturn(p1);

        EasyMock.replay(c1, c2, c3);

        TieredCompactionStrategy strategy = newStrategy(c1, c2, c3);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        CompactionPlan actualPlan = strategy.getCompactionPlan(request);

        assertNotNull(actualPlan);
        assertSame(p1, actualPlan);
        EasyMock.verify(c1, c2, c3);
    }

    @Test
    public void getCompactionPlanSkipsWhenEmptyInputAndStopsAtTier() throws IOException {
        CompactionPlan p1 = new CompactionPlan();
        p1.inputFiles.add(new StoredTabletFile("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf"));

        CompactionStrategy c1 = EasyMock.createMock("c1", CompactionStrategy.class);
        CompactionStrategy c2 = EasyMock.createMock("c2", CompactionStrategy.class);
        CompactionStrategy c3 = EasyMock.createMock("c3", CompactionStrategy.class);

        EasyMock.expect(c1.getCompactionPlan(EasyMock.anyObject())).andReturn(new CompactionPlan());
        EasyMock.expect(c2.getCompactionPlan(EasyMock.anyObject())).andReturn(p1);

        EasyMock.replay(c1, c2, c3);

        TieredCompactionStrategy strategy = newStrategy(c1, c2, c3);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        CompactionPlan actualPlan = strategy.getCompactionPlan(request);

        assertNotNull(actualPlan);
        assertSame(p1, actualPlan);
        EasyMock.verify(c1, c2, c3);
    }

    @Test
    public void getCompactionPlanWillReturnNullIfNonCompact() throws IOException {
        CompactionStrategy c1 = EasyMock.createMock("c1", CompactionStrategy.class);
        CompactionStrategy c2 = EasyMock.createMock("c2", CompactionStrategy.class);
        CompactionStrategy c3 = EasyMock.createMock("c3", CompactionStrategy.class);

        EasyMock.expect(c1.getCompactionPlan(EasyMock.anyObject())).andReturn(null);
        EasyMock.expect(c2.getCompactionPlan(EasyMock.anyObject())).andReturn(null);
        EasyMock.expect(c3.getCompactionPlan(EasyMock.anyObject())).andReturn(null);

        EasyMock.replay(c1, c2, c3);

        TieredCompactionStrategy strategy = newStrategy(c1, c2, c3);
        CompactionRequestBuilder builder = new CompactionRequestBuilder();
        MajorCompactionRequest request = builder.build();

        CompactionPlan actualPlan = strategy.getCompactionPlan(request);

        assertNull(actualPlan);
        EasyMock.verify(c1, c2, c3);
    }

    private static TieredCompactionStrategy newStrategy(CompactionStrategy... strategies) {
        Collection<TieredCompactorSupplier> mockSuppliers = new ArrayList<>();
        for (CompactionStrategy s : strategies) {
            mockSuppliers.add(new MockSupplier(s));
        }
        // @formatter:off
        TieredCompactionStrategy strategy = EasyMock.partialMockBuilder(TieredCompactionStrategy.class)
                .withConstructor()
                .addMockedMethod("createSuppliers")
                .createMock();

        // @formatter:on
        strategy.createSuppliers(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> strategy.compactors.addAll(mockSuppliers));

        EasyMock.replay(strategy);

        strategy.init(Collections.emptyMap());

        return strategy;
    }

    static class MockSupplier implements TieredCompactorSupplier {

        private final CompactionStrategy strategy;

        MockSupplier(CompactionStrategy strategy) {
            this.strategy = strategy;
        }

        @Override
        public Map<String,String> options() {
            return Collections.emptyMap();
        }

        @Override
        public CompactionStrategy get(Map<String,String> tableProperites) {
            return strategy;
        }
    }
}
