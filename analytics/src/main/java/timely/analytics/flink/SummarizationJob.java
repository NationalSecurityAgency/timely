package timely.analytics.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.response.MetricResponse;

public class SummarizationJob {

    private static final Logger LOG = LoggerFactory.getLogger(SummarizationJob.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: timely.analytics.flink.SummarizationJob <path_to_properties_file>");
            System.exit(1);
        }

        LOG.info("Loading properties from: {}", args[0]);
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        final SummarizationJobParameters jp = new SummarizationJobParameters(params);
        LOG.info("Properties: {}", jp);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // @formatter:off
        env.getConfig().setGlobalJobParameters(jp);
        env.getConfig().setAutoWatermarkInterval(60000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new SubscriptionSource(jp), "Timely Metrics Subscription")
            .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<MetricResponse>() {

                private final Logger LOG = LoggerFactory
                        .getLogger("timely.analytics.SummarizationJob.AssignerWithPeriodicWatermarks");
                private static final long serialVersionUID = 1L;
                private final Map<String, Long> metricTimes = new HashMap<>();

                @Override
                public long extractTimestamp(MetricResponse metric, long previousElementTimestamp) {
                    LOG.trace("Extracting timestamp for {}, setting to {}", metric.getMetric(),
                            metric.getTimestamp());
                    metricTimes.put(metric.getMetric(), metric.getTimestamp());
                    return metric.getTimestamp();
                }

                private long getMinWatermark() {
                    if (metricTimes.size() == 0) {
                        return -1;
                    }
                    long result = -1L;
                    for (Entry<String, Long> e : metricTimes.entrySet()) {
                        result = Math.max(e.getValue(), result);
                    }
                    return result;
                }

                @Override
                public Watermark getCurrentWatermark() {
                    long w = getMinWatermark();
                    if (w == -1) {
                        LOG.trace("Returning null watermark");
                        return null;
                    } else {
                        LOG.trace("Returning new watermark: {}", w);
                        return new Watermark(w);
                    }
                }

            })
            // partition by unique metric
            .keyBy(new MetricKeySelector())
            // one day window
            .window(TumblingEventTimeWindows.of(jp.getSummarizationInterval()))
            .fold(new MetricHistogram(), new FoldFunction<MetricResponse, MetricHistogram>() {

                private final Logger LOG = LoggerFactory
                        .getLogger("timely.analytics.SummarizationJob.FoldFunction@");
                private static final long serialVersionUID = 1L;

                @Override
                public MetricHistogram fold(MetricHistogram histo, MetricResponse metric) throws Exception {
                    if (!histo.isInitialized()) {
                        histo.initialize(metric.getMetric(), metric.getTags());
                    } else {
                        if (!histo.getMetric().equals(metric.getMetric())) {
                            throw new RuntimeException("Metric name does not match - partitioning problem");
                        }
                        if (!histo.getTags().equals(metric.getTags())) {
                            throw new RuntimeException("Tags don't not match - partitioning problem");
                        }
                    }
                    LOG.trace("Updating histogram for {} with value {} at {} for metric {}", histo.getMetric()
                            + histo.getTags(), metric.getValue(), metric.getTimestamp(), metric.getMetric()
                            + metric.getTags());
                    histo.update(metric.getValue(), metric.getTimestamp());
                    return histo;
                }
            })
            .name("Metric Windows")
            .addSink(
        		new SocketClientSink<MetricHistogram>(jp.getTimelyHostname(), jp.getTimelyTcpPort(),
                        new MetricHistogram(), 2, true) {

                    private final Logger LOG = LoggerFactory
                            .getLogger("timely.analytics.SummarizationJob.SocketClientSink");
                    private static final long serialVersionUID = 1L;
                    private final LongCounter sinkCounter = new LongCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.getRuntimeContext().addAccumulator("sink counter", this.sinkCounter);
                        LOG.info("Sink opened");
                    }

                    @Override
                    public void invoke(MetricHistogram histo) throws Exception {
                        histo.done();
                        super.invoke(histo);
                        this.sinkCounter.add(1);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        LOG.info("Sink closed");
                    }

                }).name("Timely Metric Sink");
     // @formatter:on
        env.execute("Metric Summarization");
    }
}
