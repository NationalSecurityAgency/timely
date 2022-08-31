package timely.balancer;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackages = {"timely.balancer", "timely.common"}, exclude = {ErrorMvcAutoConfiguration.class})
public class TimelyBalancer {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TimelyBalancer.class).web(WebApplicationType.NONE).run(args);
    }
}
