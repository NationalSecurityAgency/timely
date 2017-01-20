package timely.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Sum;

public class DownsampleTest {

    @Test
    public void simple() {
        Downsample dsample = new Downsample(10, 30, 1, new Avg());
        for (int i = 10; i < 30; i++) {
            dsample.add(i, i - 10);
        }
        int i = 0;
        for (Sample sample : dsample) {
            assertEquals(10 + i, sample.timestamp);
            assertTrue(sample.timestamp < 30);
            assertEquals(i, (int) sample.value);
            i++;
        }
        assertEquals(20, i);
        dsample = new Downsample(10, 100, 7, new Sum());
        for (int j = 0; j < 5; j++) {
            for (int k = 10; k < 100; k++) {
                dsample.add(k, j + 0.);
            }
        }
        i = 0;
        for (Sample sample : dsample) {
            assertEquals((1 + 2 + 3 + 4) * Math.min(7, (100 - (10 + i * 7))), sample.value, 0.0D);
            assertEquals(10 + i * 7, sample.timestamp);
            i++;
        }
        assertEquals((100 - 10) / 7 + 1, i);
        dsample = new Downsample(10, 30, 10, new Avg());
        for (int j = 10; j < 30; j++) {
            for (int k = 0; k < 10; k++) {
                dsample.add(j, k + 0.);
            }
        }
        for (int j = 0; j < 100; j++) {
            dsample.add(15, 0);
        }
        i = 0;
        for (Sample sample : dsample) {
            if (i == 0) {
                assertEquals(2.25, sample.value, 0.0D);
            } else {
                assertEquals(4.5, sample.value, 0.0D);
            }
            assertEquals(10 * i + 10, sample.timestamp);
            i++;
        }
        assertEquals(2, i);
    }

    @Test
    public void testCombineTrivial() throws Exception {
        Downsample ds = new Downsample(0, 1000, 100, new Avg());
        for (int i = 0; i < 1000; i += 100) {
            ds.add(i, .2);
        }
        Downsample result = Downsample.combineDownsample(Collections.singleton(ds), null);
        int count = 0;
        for (Sample s : result) {
            assertEquals(.2, s.value, 0.0D);
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testCombineMissingReport() throws Exception {
        Downsample ds = new Downsample(0, 1000, 100, new Avg());
        for (int i = 0; i < 1000; i += 100) {
            if (i != 700) {
                ds.add(i, .2);
            }
        }
        Downsample result = Downsample.combineDownsample(Collections.singleton(ds), null);
        int count = 0;
        for (Sample s : result) {
            assertEquals(.2, s.value, 0.0D);
            count++;
        }
        assertEquals(9, count);
    }

    @Test
    public void testDownsampleStartCalculation() throws Exception {
        long queryStart = System.currentTimeMillis() - 86400000;
        long period = 60000;
        long keyTimestamp = queryStart + (86400000 / 2 + 3256);

        Set<Long> expectedStartTimes = new HashSet<>();
        for (long i = queryStart; i < queryStart + 86400000; i += period) {
            expectedStartTimes.add(i);
        }
        assertEquals(1440, expectedStartTimes.size());

        long sampleStart = keyTimestamp - ((keyTimestamp - queryStart) % period);
        assertTrue(expectedStartTimes.contains(sampleStart));

    }

}
