package timely.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Value consists of a long timestamp and a double measurement
 */
public class Value implements Comparator<Value>, Serializable {

    private Long timestamp;
    private Double measure;

    public Value() {
    }

    public Value(Value other) {
        this.setTimestamp(other.getTimestamp());
        this.setMeasure(other.getMeasure());
    }

    @JsonCreator
    public Value(@JsonProperty("timestamp") Long timestamp, @JsonProperty("measure") Double measure) {
        this.setTimestamp(timestamp);
        this.setMeasure(measure);
    }

    @JsonProperty("timestamp")
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Long timestamp) {
        this.setTimestamp(timestamp, TimeUnit.MILLISECONDS);
    }

    public void setTimestamp(final Long timestamp, TimeUnit timeUnit) {
        this.timestamp = timeUnit.toMillis(timestamp);
    }

    @JsonProperty("measure")
    public Double getMeasure() {
        return measure;
    }

    public void setMeasure(final Double measure) {
        this.measure = measure;
    }

    @Override
    /**
     * Compares timestamps for sorting.
     *
     * Note: this comparator imposes orderings that are inconsistent with equals
     *
     */
    public int compare(Value v1, Value v2) {
        if (v1.getTimestamp().equals(v2.getTimestamp()))
            return 0;
        return v1.getTimestamp() > v2.getTimestamp() ? 1 : -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (null == o || getClass() != o.getClass())
            return false;

        if (getTimestamp() != null ? !getTimestamp().equals(((Value) o).getTimestamp())
                : ((Value) o).getTimestamp() != null)
            return false;
        return !(getMeasure() != null ? !getMeasure().equals(((Value) o).getMeasure())
                : ((Value) o).getMeasure() != null);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timestamp, measure);
    }

    @Override
    public String toString() {
        return "Value{" + "timestamp=" + timestamp + ", measure=" + measure + "}";
    }
}
