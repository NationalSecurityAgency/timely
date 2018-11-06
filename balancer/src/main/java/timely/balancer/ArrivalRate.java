package timely.balancer;

import java.util.Timer;
import java.util.TimerTask;

public class ArrivalRate {

    private long shortTermArrivals = 0l;
    private double shortTermRate = 0;
    private long shortTermLastReset = 0l;

    private long mediumTermArrivals = 0l;
    private double mediumTermRate = 0;
    private long mediumTermLastReset = 0l;

    private long longTermArrivals = 0l;
    private double longTermRate = 0;
    private long longTermLastReset = 0l;

    private double rate = 0;

    private boolean recalculateEveryUpdate = true;
    private long created = System.currentTimeMillis();

    private Timer timer = new Timer("ArrivalRateTimer");

    public ArrivalRate() {
        shortTermLastReset = mediumTermLastReset = longTermLastReset = System.currentTimeMillis();

        // 5 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetShort();
            }
        }, 300000, 300000);

        // 5 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetMedium();
            }
        }, 900000, 900000);

        // 5 minute
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                resetLong();
            }
        }, 3600000, 3600000);
    }

    synchronized private void resetShort() {
        long now = System.currentTimeMillis();
        shortTermRate = shortTermArrivals / (((double) (now - shortTermLastReset)) / 1000);
        shortTermArrivals = 0;
        shortTermLastReset = now;
    }

    synchronized private void resetMedium() {
        long now = System.currentTimeMillis();
        mediumTermRate = mediumTermArrivals / (((double) (now - mediumTermLastReset)) / 1000);
        mediumTermArrivals = 0;
        mediumTermLastReset = now;
    }

    synchronized private void resetLong() {
        long now = System.currentTimeMillis();
        longTermRate = longTermArrivals / (((double) (now - longTermLastReset)) / 1000);
        longTermArrivals = 0;
        longTermLastReset = now;
    }

    synchronized public void arrived() {
        shortTermArrivals++;
        mediumTermArrivals++;
        longTermArrivals++;
        if (recalculateEveryUpdate) {
            calculateRate();
            if ((System.currentTimeMillis() - created) > 600000) {
                recalculateEveryUpdate = false;
            }
        }
    }

    synchronized private double getShortRate(long now) {
        if (shortTermRate > 0) {
            return shortTermRate;
        } else {
            return shortTermArrivals / (((double) (now - shortTermLastReset)) / 1000);
        }
    }

    synchronized private double getMediumRate(long now) {
        if (mediumTermRate > 0) {
            return mediumTermRate;
        } else {
            return mediumTermArrivals / (((double) (now - mediumTermLastReset)) / 1000);
        }
    }

    synchronized private double getLongRate(long now) {
        if (longTermRate > 0) {
            return longTermRate;
        } else {
            return longTermArrivals / (((double) (now - longTermLastReset)) / 1000);
        }
    }

    synchronized public void calculateRate() {
        long now = System.currentTimeMillis();
        // 1 minute
        if (now - shortTermLastReset > 60000) {
            // use live
            shortTermRate = 0;
        }
        // 3 minute
        if (now - mediumTermLastReset > 180000) {
            // use live
            mediumTermRate = 0;
        }
        // 12 minute
        if (now - longTermLastReset > 720000) {
            // use live
            longTermRate = 0;
        }
        rate = 0.50 * getLongRate(now) + 0.30 * getMediumRate(now) + 0.20 * getShortRate(now);
    }

    synchronized public double getRate() {
        if (rate == 0) {
            calculateRate();
        }
        return rate;
    }
}
