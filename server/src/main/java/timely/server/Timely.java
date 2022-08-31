package timely.server;

import timely.application.metrics.Metrics;
import timely.application.tablet.Tablet;
import timely.application.testingest.TestIngest;
import timely.application.testquery.TestQuery;

public class Timely {
    public static void main(String[] args) {
        String applicationName = args.length >= 1 ? args[0] : "";
        switch (applicationName.toLowerCase()) {
            case "metrics":
                Metrics.main(args);
                break;
            case "tablet":
                Tablet.main(args);
                break;
            case "testingest":
                TestIngest.main(args);
                break;
            case "testquery":
                TestQuery.main(args);
                break;
            default:
                TimelyServer.main(args);
        }
    }
}
