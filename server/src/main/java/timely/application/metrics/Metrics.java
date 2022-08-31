package timely.application.metrics;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

import timely.common.configuration.TimelyCommonConfiguration;

@SpringBootApplication(scanBasePackageClasses = {TimelyCommonConfiguration.class, Metrics.class}, exclude = {ErrorMvcAutoConfiguration.class})
public class Metrics {

    public static void main(String[] args) {
        new SpringApplicationBuilder(Metrics.class).main(Metrics.class).web(WebApplicationType.NONE).run(args);
    }
}
