package timely;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return "StandaloneServer <configFile> <directory>";
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println(usage());
        }
        final File conf = new File(args[0]);
        if (!conf.canRead()) {
            throw new RuntimeException("Configuration file does not exist or cannot be read");
        }
        File tmp = new File(args[1]);
        if (!tmp.canWrite()) {
            System.err.println("Unable to write to directory: " + tmp);
            System.exit(1);
        }
        File accumuloDir = new File(tmp, "accumulo");
        MiniAccumuloConfig macConfig = new MiniAccumuloConfig(accumuloDir, "secret");
        macConfig.setInstanceName("TimelyStandalone");
        macConfig.setZooKeeperPort(9804);
        macConfig.setNumTservers(1);
        macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
        try {
            mac = new MiniAccumuloCluster(macConfig);
            mac.start();
            mac.getInstanceName();
        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        }
        try {
            new StandaloneServer(conf);
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
