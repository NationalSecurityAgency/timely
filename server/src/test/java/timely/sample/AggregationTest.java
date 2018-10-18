package timely.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Sum;

public class AggregationTest {

    @Test
    public void simple() {
        Aggregation asample = new Aggregation(new Avg());
        for (int i = 10; i < 30; i++) {
            asample.add(i, i - 10);
        }
        for (int i = 10; i < 30; i++) {
            asample.add(i, i);
        }
        int i = 0;
        for (Sample sample : asample) {
            assertEquals(10 + i, sample.timestamp);
            assertTrue(sample.timestamp < 30);
            assertEquals(i + 5, (int) sample.value);
            i++;
        }
        assertEquals(20, i);
        asample = new Aggregation(new Sum());
        for (int j = 0; j < 5; j++) {
            for (int k = 10; k < 100; k++) {
                asample.add(k, j + 0.);
            }
        }
        i = 0;
        for (Sample sample : asample) {
            assertEquals(10 + i, sample.timestamp);
            assertEquals((1 + 2 + 3 + 4), sample.value, 0.0D);
            i++;
        }
        assertEquals(100 - 10, i);
    }

}
