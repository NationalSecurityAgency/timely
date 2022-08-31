package timely.application.metrics;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import timely.common.configuration.TimelyProperties;
import timely.model.Meta;

@Component
public class MetricsRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MetricsRunner.class);

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AccumuloClient accumuloClient;

    @Autowired
    private TimelyProperties timelyProperties;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        try (Scanner s = accumuloClient.createScanner(timelyProperties.getMetaTable(),
                        accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami()))) {

            s.setRange(new Range(Meta.METRIC_PREFIX, true, Meta.TAG_PREFIX, false));
            for (Entry<Key,Value> e : s) {
                System.out.println(e.getKey().getRow().toString().substring(Meta.METRIC_PREFIX.length()));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        SpringApplication.exit(context);
    }
}
