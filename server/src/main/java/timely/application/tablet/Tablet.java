package timely.application.tablet;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

import timely.common.configuration.TimelyCommonConfiguration;

@SpringBootApplication(scanBasePackageClasses = {TimelyCommonConfiguration.class, Tablet.class}, exclude = {ErrorMvcAutoConfiguration.class})
public class Tablet {

    public static void main(String[] args) {
        new SpringApplicationBuilder(Tablet.class).main(Tablet.class).web(WebApplicationType.NONE).run(args);
    }
}
