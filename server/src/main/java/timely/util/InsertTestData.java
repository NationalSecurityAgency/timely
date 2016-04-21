package timely.util;

import java.io.PrintStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTestData {

    private static final Logger LOG = LoggerFactory.getLogger(InsertTestData.class);

    private static final List<String> METRICS = new ArrayList<>();
    static {
        METRICS.add("sys.cpu0.user");
        METRICS.add("sys.cpu0.idle");
        METRICS.add("sys.cpu0.system");
        METRICS.add("sys.cpu0.wait");
        METRICS.add("sys.memory.free");
        METRICS.add("sys.memory.used");
        METRICS.add("sys.eth0.rx-errors");
        METRICS.add("sys.eth0.rx-dropped");
        METRICS.add("sys.eth0.rx-packets");
        METRICS.add("sys.eth0.tx-errors");
        METRICS.add("sys.eth0.tx-dropped");
        METRICS.add("sys.eth0.tx-packets");
        METRICS.add("sys.swap.free");
        METRICS.add("sys.swap.used");
    }

    private static final List<String> HOSTS = new ArrayList<>();

    static {
        HOSTS.add("n01");
        HOSTS.add("n02");
    }

    private static final List<String> RACKS = new ArrayList<>();

    static {
        RACKS.add("r01");
        RACKS.add("r02");
    }

    private static final String FMT = "put {0} {1,number,#} {2,number} host={4}{3} rack={4}";

    private static String usage() {
        return "InsertTestData <host> <port> [--fast]";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println(usage());
            System.exit(1);
        }
        String hostname = args[0];
        String port = args[1];
        boolean goFast = false;
        if (args.length == 3) {
            if (args[2].equalsIgnoreCase("--fast")) {
                goFast = true;
            }
        }

        try (Socket sock = new Socket(hostname, Integer.parseInt(port));
                PrintStream writer = new PrintStream(sock.getOutputStream(), true, StandardCharsets.UTF_8.name());) {
            while (true) {
                long time = System.currentTimeMillis();
                METRICS.forEach(m -> {
                    RACKS.forEach(rack -> {
                        HOSTS.forEach(host -> {
                            String put = MessageFormat.format(FMT, m, time,
                                    ThreadLocalRandom.current().nextDouble(0.0D, 100.0D), host, rack);
                            writer.println(put);
                            LOG.info(put);
                        });
                    });
                });
                writer.flush();
                if (!goFast) {
                    Thread.sleep(10 * 1000);
                } else {
                    Thread.sleep(500);
                }
            }
        }
    }

}
