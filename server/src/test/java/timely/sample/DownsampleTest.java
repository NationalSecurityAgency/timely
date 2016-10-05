package timely.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import timely.api.request.timeseries.QueryRequest.RateOption;
import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Max;
import timely.sample.aggregators.Min;
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
        Downsample result = Downsample.combine(Collections.singleton(ds), null);
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
        Downsample result = Downsample.combine(Collections.singleton(ds), null);
        int count = 0;
        for (Sample s : result) {
            assertEquals(.2, s.value, 0.0D);
            count++;
        }
        assertEquals(9, count);
    }

    // @Test
    // public void testInterpolation() throws Exception {
    // Downsample ds = new Downsample(0, 1000, 100, new Avg());
    // ds.add(0, .0);
    // ds.add(900, .9);
    // Downsample result = Downsample.combine(Collections.singleton(ds), null);
    // int count = 0;
    // for (Sample s : result) {
    // assertEquals(count * .1, s.value, 0.0001);
    // count++;
    // }
    // assertEquals(10, count);
    // }

    // @Test
    // public void testInterpolationX2() throws Exception {
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // ds1.add(0, .0);
    // ds1.add(1000, 1.);
    // Downsample ds2 = new Downsample(0, 1000, 100, new Avg());
    // ds2.add(0, 1.);
    // ds2.add(1000, .0);
    // Downsample result = Downsample.combine(Arrays.asList(ds1, ds2), null);
    // int count = 0;
    // for (Sample s : result) {
    // assertEquals(.5, s.value, 0.0001);
    // count++;
    // }
    // assertEquals(11, count);
    // }

    // @Test
    // public void testInterpolationMissing() throws Exception {
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, i * .1);
    // }
    // Downsample ds2 = new Downsample(0, 1000, 100, new Avg());
    // for (int i = 0; i <= 1000; i += 100) {
    // if (i != 7) {
    // ds1.add(i, 1 - i * .1);
    // }
    // }
    // Downsample result = Downsample.combine(Arrays.asList(ds1, ds2), null);
    // int count = 0;
    // for (Sample s : result) {
    // assertEquals(.5, s.value, 0.0001);
    // count++;
    // }
    // assertEquals(11, count);
    // }

    // @Test
    // public void testCounterToRate() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n += 17);
    // }
    // Downsample result = Downsample.combine(Collections.singleton(ds1),
    // counter);
    // int count = 0;
    // for (Sample s : result) {
    // assertEquals(17, s.value, 0.0001);
    // count++;
    // }
    // assertEquals(10, count);
    // }

    // @Test
    // public void testDecreasingRate() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(false);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 0.75D);
    // }
    // Downsample result = Downsample.combine(Collections.singleton(ds1),
    // counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(750.0D, i.next().value, 0.0D);
    // assertEquals(562.0D, i.next().value, 0.0D);
    // assertEquals(421.0D, i.next().value, 0.0D);
    // assertEquals(315.0D, i.next().value, 0.0D);
    // assertEquals(236.0D, i.next().value, 0.0D);
    // assertEquals(177.0D, i.next().value, 0.0D);
    // assertEquals(132.0D, i.next().value, 0.0D);
    // assertEquals(99.0D, i.next().value, 0.0D);
    // assertEquals(74.0D, i.next().value, 0.0D);
    // assertEquals(55.0D, i.next().value, 0.0D);
    // assertEquals(41.0D, i.next().value, 0.0D);
    // assertEquals(Double.NaN, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testIncreasingCounterToRate() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 1.25D);
    // }
    // Downsample result = Downsample.combine(Collections.singleton(ds1),
    // counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(312.0D, i.next().value, 0.0D);
    // assertEquals(390.0D, i.next().value, 0.0D);
    // assertEquals(488.0D, i.next().value, 0.0D);
    // assertEquals(610.0D, i.next().value, 0.0D);
    // assertEquals(762.0D, i.next().value, 0.0D);
    // assertEquals(953.0D, i.next().value, 0.0D);
    // assertEquals(1191.0D, i.next().value, 0.0D);
    // assertEquals(1489.0D, i.next().value, 0.0D);
    // assertEquals(1861.0D, i.next().value, 0.0D);
    // assertEquals(2326.0D, i.next().value, 0.0D);
    // assertEquals(Double.NaN, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testRestartCounterToRate() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    // counter.setCounterMax(1000000);
    // counter.setResetValue(0);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // if (i == 500) {
    // ds1.add(i, 0.0D);
    // n = 1000;
    // } else {
    // ds1.add(i, n *= 1.1D);
    // }
    // }
    // Downsample result = Downsample.combine(Collections.singleton(ds1),
    // counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(110.0D, i.next().value, 0.0D);
    // assertEquals(121.0D, i.next().value, 0.0D);
    // assertEquals(133.0D, i.next().value, 0.0D);
    // assertEquals(146.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(1100.0D, i.next().value, 0.0D);
    // assertEquals(110.0D, i.next().value, 0.0D);
    // assertEquals(121.0D, i.next().value, 0.0D);
    // assertEquals(133.0D, i.next().value, 0.0D);
    // assertEquals(146.0D, i.next().value, 0.0D);
    // assertEquals(Double.NaN, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testDecreasingCounterToRate() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    // counter.setCounterMax(1000000);
    // counter.setResetValue(0);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 0.75D);
    // }
    // Downsample result = Downsample.combine(Collections.singleton(ds1),
    // counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(Double.NaN, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testCounterToRatesCombinedMin() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Min());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 0.75D);
    // }
    // Downsample ds2 = new Downsample(0, 1000, 100, new Min());
    // n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds2.add(i, n *= 1.25D);
    // }
    // Downsample result = Downsample.combine(Arrays.asList(ds1, ds2), counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(-1.0D, i.next().value, 0.0D);
    // assertEquals(0.0D, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testCounterToRatesCombinedMax() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Max());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 0.75D);
    // }
    // Downsample ds2 = new Downsample(0, 1000, 100, new Max());
    // n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds2.add(i, n *= 1.25D);
    // }
    // Downsample result = Downsample.combine(Arrays.asList(ds1, ds2), counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(312.0D, i.next().value, 0.0D);
    // assertEquals(390.0D, i.next().value, 0.0D);
    // assertEquals(488.0D, i.next().value, 0.0D);
    // assertEquals(610.0D, i.next().value, 0.0D);
    // assertEquals(762.0D, i.next().value, 0.0D);
    // assertEquals(953.0D, i.next().value, 0.0D);
    // assertEquals(1191.0D, i.next().value, 0.0D);
    // assertEquals(1489.0D, i.next().value, 0.0D);
    // assertEquals(1861.0D, i.next().value, 0.0D);
    // assertEquals(2326.0D, i.next().value, 0.0D);
    // assertEquals(0.0D, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

    // @Test
    // public void testCounterToRatesCombinedAvg() throws Exception {
    // RateOption counter = new RateOption();
    // counter.setCounter(true);
    //
    // Downsample ds1 = new Downsample(0, 1000, 100, new Avg());
    // int n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds1.add(i, n *= 0.75D);
    // }
    // Downsample ds2 = new Downsample(0, 1000, 100, new Avg());
    // n = 1000;
    // for (int i = 0; i <= 1000; i += 100) {
    // ds2.add(i, n *= 1.25D);
    // }
    // Downsample result = Downsample.combine(Arrays.asList(ds1, ds2), counter);
    // Iterator<Sample> i = result.iterator();
    // assertEquals(155.5D, i.next().value, 0.0D);
    // assertEquals(194.5D, i.next().value, 0.0D);
    // assertEquals(243.5D, i.next().value, 0.0D);
    // assertEquals(304.5D, i.next().value, 0.0D);
    // assertEquals(380.5D, i.next().value, 0.0D);
    // assertEquals(476.0D, i.next().value, 0.0D);
    // assertEquals(595.0D, i.next().value, 0.0D);
    // assertEquals(744.0D, i.next().value, 0.0D);
    // assertEquals(930.0D, i.next().value, 0.0D);
    // assertEquals(1162.5D, i.next().value, 0.0D);
    // assertEquals(Double.NaN, i.next().value, 0.0D);
    // assertFalse(i.hasNext());
    // }

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
