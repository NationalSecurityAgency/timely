package timely.analytics.flink;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.addSource(new SubscriptionSource(jp), "Timely Metrics Subscription")
        // partition by unique metric
        .keyBy(new MetricKeySelector())
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
                private final SortedStringAccumulator dateTimeAccumulator = new SortedStringAccumulator();
                private final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH");

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    this.getRuntimeContext().addAccumulator("sink counter", this.sinkCounter);
                    this.getRuntimeContext().addAccumulator("sink hourly count", dateTimeAccumulator);
                    LOG.info("Sink opened");
                }

                @Override
                public void invoke(MetricHistogram histo) throws Exception {
                    histo.done();
                    super.invoke(histo);
                    this.sinkCounter.add(1);
                    this.dateTimeAccumulator.add(formatter.format(new Date(histo.getTimestamp())));
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    LOG.info("Sink closed");
                }

            }).name("Timely Metric Sink");
        // @formatter:on
        env.execute("Metric Summarization");
        // Looks like there is no way to stop Flink job if underlying web socket
        // is closed.
    }
}
