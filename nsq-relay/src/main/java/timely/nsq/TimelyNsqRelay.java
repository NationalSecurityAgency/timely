package timely.nsq;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication(scanBasePackages = {"timely.nsq"}, exclude = {ErrorMvcAutoConfiguration.class})
public class TimelyNsqRelay {

    public static void main(String[] args) {
        new SpringApplicationBuilder(TimelyNsqRelay.class).main(TimelyNsqRelay.class).web(WebApplicationType.NONE).run(args);
    }
}
