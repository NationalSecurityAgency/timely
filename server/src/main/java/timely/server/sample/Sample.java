package timely.server.sample;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class Sample {

    private long timestamp;
    private double value;

    public Sample() {}

    public Sample(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public void set(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public double getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("timestamp", this.timestamp);
        tsb.append("value", this.value);
        return tsb.toString();
    }
}
