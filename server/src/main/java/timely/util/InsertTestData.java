package timely.util;

import java.io.PrintStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTestData {

    private static final Logger LOG = LoggerFactory.getLogger(InsertTestData.class);

    private static final List<String> METRICS = new ArrayList<>();
    static {
        METRICS.add("sys.cpu.user");
        METRICS.add("sys.cpu.idle");
        METRICS.add("sys.cpu.system");
        METRICS.add("sys.cpu.wait");
        METRICS.add("sys.cache.free");
        METRICS.add("sys.cache.used");
        METRICS.add("sys.eth.rx-errors");
        METRICS.add("sys.eth.rx-dropped");
        METRICS.add("sys.eth.rx-packets");
        METRICS.add("sys.eth.tx-errors");
        METRICS.add("sys.eth.tx-dropped");
        METRICS.add("sys.eth.tx-packets");
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

    private static final List<String> INSTANCES = new ArrayList<>();
    static {
        INSTANCES.add("0");
        INSTANCES.add("1");
        INSTANCES.add("2");
        INSTANCES.add("3");
    }

    private static final Map<String, String> VISIBILITIES = new HashMap<>();
    static {
        VISIBILITIES.put("sys.eth.rx-errors", "A");
        VISIBILITIES.put("sys.eth.rx-dropped", "B");
        VISIBILITIES.put("sys.eth.rx-packets", "C");
        VISIBILITIES.put("sys.eth.tx-errors", "D");
        VISIBILITIES.put("sys.eth.tx-dropped", "E");
        VISIBILITIES.put("sys.eth.tx-packets", "F");
    }

    private static final String FMT = "put {0} {1,number,#} {2,number} host={4}{3} rack={4} instance={5}";

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
                    final String viz = VISIBILITIES.get(m);
                    RACKS.forEach(rack -> {
                        HOSTS.forEach(host -> {
                            INSTANCES.forEach(instance -> {
                                if (!m.startsWith("sys.cpu.") && !instance.equals("0")) {
                                    return;
                                }
                                String put = MessageFormat.format(FMT, m, time,
                                        ThreadLocalRandom.current().nextDouble(0.0D, 100.0D), host, rack, instance);
                                if (viz != null)
                                    put += " viz=" + viz;
                                writer.println(put);
                                LOG.info(put);
                            });
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
