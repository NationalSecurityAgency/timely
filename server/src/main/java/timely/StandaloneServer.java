package timely;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class StandaloneServer extends Server {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneServer.class);

    private static MiniAccumuloCluster mac = null;

    public StandaloneServer(File conf) throws Exception {
        super(conf);
    }

    @Override
    public void shutdown() {
        try {
            mac.stop();
            LOG.info("MiniAccumuloCluster shutdown.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error stopping MiniAccumuloCluster");
            e.printStackTrace();
        }
        super.shutdown();
    }

    private static String usage() {
        return "StandaloneServer <directory>";
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println(usage());
        }
        File tmp = new File(args[0]);
        if (!tmp.canWrite()) {
            System.err.println("Unable to write to directory: " + tmp);
            System.exit(1);
        }
        File accumuloDir = new File(tmp, "accumulo");
        MiniAccumuloConfig macConfig = new MiniAccumuloConfig(accumuloDir, "secret");
        macConfig.setNumTservers(1);
        macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
        try {
            mac = new MiniAccumuloCluster(macConfig);
            mac.start();
        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        }
        File configDir = new File(tmp, "conf");
        if (!configDir.mkdir()) {
            System.err.println("Error creating configuration directory: " + configDir);
            System.exit(1);
        }
        File config = new File(configDir, "timely.properties");
        try {
            try (BufferedWriter writer = Files.newWriter(config, StandardCharsets.UTF_8)) {
                writer.write(Configuration.IP + "=127.0.0.1\n");
                writer.write(Configuration.PUT_PORT + "=54321\n");
                writer.write(Configuration.QUERY_PORT + "=54322\n");
                writer.write(Configuration.ZOOKEEPERS + "=" + mac.getZooKeepers() + "\n");
                writer.write(Configuration.INSTANCE_NAME + "=" + mac.getInstanceName() + "\n");
                writer.write(Configuration.USERNAME + "=root\n");
                writer.write(Configuration.PASSWORD + "=secret\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing configuration file: " + e.getMessage());
            System.exit(1);
        }
        try {
            new StandaloneServer(config);
        } catch (Exception e) {
            System.err.println("Error starting server");
            e.printStackTrace();
            System.exit(1);
        }
        try {
            LATCH.await();
        } catch (InterruptedException e) {
            LOG.info("Server shutting down.");
        }
    }

}
