package timely.server;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackages = {"timely.server", "timely.common"}, exclude = {ErrorMvcAutoConfiguration.class})
public class TimelyServer {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TimelyServer.class).main(TimelyServer.class).web(WebApplicationType.NONE).run(args);
    }
}
