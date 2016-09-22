package timely.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 *
 *
 */
public class MetricTest {

    @Test
    public void testEquals() {
        Metric m1 = Metric.newBuilder().name("m1").tag("t1", "v1").tag("t2", "v2").value(1, 0.0).build();
        Metric m2 = Metric.newBuilder().name("m1").tag("t2", "v2").tag("t1", "v1").value(1, 0.0).build();

        assertTrue(m1.equals(m2));
        assertTrue(m2.equals(m1));

        Metric m3 = Metric.newBuilder().name("m1").tag("t1", "v1").value(1, 0.0).build();
        assertFalse(m1.equals(m3));

        Metric m4 = Metric.newBuilder().name("m4").tag("t2", "v2").tag("t1", "v1").value(1, 0.0).build();
        assertFalse(m1.equals(m4));

        Metric m5 = Metric.newBuilder().name("m1").tag("t3", "v3").tag("t2", "v2").tag("t1", "v1").value(1, 0.0)
                .build();
        assertFalse(m1.equals(m5));

        Metric m6 = Metric.newBuilder().name("m1").tag("t2", "v2").tag("t1", "v1").value(2, 0.0).build();
        assertFalse(m1.equals(m6));

        Metric m7 = Metric.newBuilder().name("m1").tag("t2", "v2").tag("t1", "v1").value(1, 1.0).build();
        assertFalse(m1.equals(m7));
    }

    @Test
    public void testJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        String expectedJson = "{\"name\":\"m1\",\"timestamp\":1,\"measure\":1.0,\"tags\":[{\"k1\":\"v1\"}]}";
        System.out.println(expectedJson);

        Metric m1 = Metric.newBuilder().name("m1").tag("k1", "v1").value(1, 1.0).build();
        System.out.println(mapper.writeValueAsString(m1)); // this will throw if
                                                           // problem with
                                                           // deserialization

        Metric expectedMetric = mapper.readValue(expectedJson, Metric.class);

        assertTrue(m1.equals(expectedMetric));

        expectedJson = "{\"name\":\"m1\",\"tags\":[{\"k1\":\"v1\"}],\"timestamp\":5,\"measure\":5.0}";
        expectedMetric = mapper.readValue(expectedJson, Metric.class);

        assertEquals((long) expectedMetric.getValue().getTimestamp(), 5L);
        assertEquals(expectedMetric.getValue().getMeasure(), 5.0D, 0.0);

    }
}
