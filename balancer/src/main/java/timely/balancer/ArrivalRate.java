package timely.balancer;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.AtomicDouble;

public class ArrivalRate {

    private AtomicLong shortTermArrivals = new AtomicLong(0);
    private AtomicDouble shortTermRate = new AtomicDouble(0);
    private AtomicLong shortTermLastReset = new AtomicLong(0);

    private AtomicLong mediumTermArrivals = new AtomicLong(0);
    private AtomicDouble mediumTermRate = new AtomicDouble(0);
    private AtomicLong mediumTermLastReset = new AtomicLong(0);

    private AtomicLong longTermArrivals = new AtomicLong(0);
    private AtomicDouble longTermRate = new AtomicDouble(0);
    private AtomicLong longTermLastReset = new AtomicLong(0);

    public ArrivalRate(Timer timer) {
        long now = System.currentTimeMillis();
        shortTermLastReset.set(now);
        mediumTermLastReset.set(now);
        longTermLastReset.set(now);

        // 2 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetShort();
            }
        }, 120000, 120000);

        // 5 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetMedium();
            }
        }, 300000, 300000);

        // 15 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetLong();
            }
        }, 900000, 900000);
    }

    private void resetShort() {
        long now = System.currentTimeMillis();
        long lastReset = shortTermLastReset.get();
        if (now == lastReset) {
            shortTermRate.set(0);
        } else {
            shortTermRate.set(shortTermArrivals.get() / (((double) (now - lastReset)) / 1000));
        }
        shortTermArrivals.set(0);
        shortTermLastReset.set(now);
    }

    private void resetMedium() {
        long now = System.currentTimeMillis();
        long lastReset = mediumTermLastReset.get();
        if (now == lastReset) {
            mediumTermRate.set(0);
        } else {
            mediumTermRate.set(mediumTermArrivals.get() / (((double) (now - lastReset)) / 1000));
        }
        mediumTermArrivals.set(0);
        mediumTermLastReset.set(now);
    }

    private void resetLong() {
        long now = System.currentTimeMillis();
        long lastReset = longTermLastReset.get();
        if (now == lastReset) {
            longTermRate.set(0);
        } else {
            longTermRate.set(longTermArrivals.get() / (((double) (now - lastReset)) / 1000));
        }
        longTermArrivals.set(0);
        longTermLastReset.set(now);
    }

    public void arrived() {
        shortTermArrivals.incrementAndGet();
        mediumTermArrivals.incrementAndGet();
        longTermArrivals.incrementAndGet();
    }

    private double getShortRate(long now) {
        double rate = shortTermRate.get();
        long lastReset = shortTermLastReset.get();
        if (rate > 0 || now == lastReset) {
            return rate;
        } else {
            return shortTermArrivals.get() / (((double) (now - lastReset)) / 1000);
        }
    }

    private double getMediumRate(long now) {
        double rate = mediumTermRate.get();
        long lastReset = mediumTermLastReset.get();
        if (rate > 0 || now == lastReset) {
            return rate;
        } else {
            return mediumTermArrivals.get() / (((double) (now - lastReset)) / 1000);
        }
    }

    private double getLongRate(long now) {
        double rate = longTermRate.get();
        long lastReset = longTermLastReset.get();
        if (rate > 0 || now == lastReset) {
            return rate;
        } else {
            return longTermArrivals.get() / (((double) (now - lastReset)) / 1000);
        }
    }

    public double getRate() {
        long now = System.currentTimeMillis();
        return 0.10 * getLongRate(now) + 0.10 * getMediumRate(now) + 0.80 * getShortRate(now);
    }
}
