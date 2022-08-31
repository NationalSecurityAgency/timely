package timely.application.tablet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import timely.application.metrics.Metrics;
import timely.common.configuration.TimelyProperties;
import timely.server.configuration.TabletMetadataProperties;
import timely.server.store.compaction.util.TabletMetadataQuery;
import timely.server.store.compaction.util.TabletMetadataView;

@Component
@EnableConfigurationProperties(TabletMetadataProperties.class)
public class TabletRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    @Autowired
    private ApplicationContext context;

    @Autowired
    private AccumuloClient accumuloClient;

    @Autowired
    private TimelyProperties timelyProperties;

    @Autowired
    private TabletMetadataProperties tabletMetadataProperties;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        try {
            TabletMetadataQuery query = new TabletMetadataQuery(accumuloClient, timelyProperties, tabletMetadataProperties);
            TabletMetadataView view = query.run();
            System.out.println(view.toText(tabletMetadataProperties.getTimeUnit()));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        SpringApplication.exit(context);
    }
}
