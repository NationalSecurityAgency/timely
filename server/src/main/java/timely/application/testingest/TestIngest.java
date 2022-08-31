package timely.application.testingest;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

import timely.common.configuration.TimelyCommonConfiguration;

@SpringBootApplication(scanBasePackageClasses = {TimelyCommonConfiguration.class, TestIngest.class}, exclude = {ErrorMvcAutoConfiguration.class})
public class TestIngest {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TestIngest.class).main(TestIngest.class).web(WebApplicationType.NONE).run(args);
    }
}
