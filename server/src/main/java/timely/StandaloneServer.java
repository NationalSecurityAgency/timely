package timely;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneServer extends Server {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneServer.class);

    private static MiniAccumuloCluster mac = null;

    public StandaloneServer(Configuration conf) throws Exception {
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
        if (args.length < 1) {
            System.err.println(usage());
        }
        File tmp = new File(args[0]);
        if (!tmp.canWrite()) {
            System.err.println("Unable to write to directory: " + tmp);
            System.exit(1);
        }

        Configuration conf = initializeConfiguration(args);

        File accumuloDir = new File(tmp, "accumulo");
        MiniAccumuloConfig macConfig = new MiniAccumuloConfig(accumuloDir, "secret");
        macConfig.setInstanceName("TimelyStandalone");
        macConfig.setZooKeeperPort(9804);
        macConfig.setNumTservers(1);
        macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
        try {
            mac = new MiniAccumuloCluster(macConfig);
            LOG.info("Starting MiniAccumuloCluster");
            mac.start();
            LOG.info("MiniAccumuloCluster started.");
            String instanceName = mac.getInstanceName();
            LOG.info("MiniAccumuloCluster instance name: {}", instanceName);

        } catch (IOException | InterruptedException e) {
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        }
        try {
            Connector conn = mac.getConnector("root", "secret");
            SecurityOperations sops = conn.securityOperations();
            Authorizations rootAuths = new Authorizations("A", "B", "C", "D", "E", "F", "G", "H", "I");
            sops.changeUserAuthorizations("root", rootAuths);
        } catch (AccumuloException | AccumuloSecurityException e) {
            System.err.println("Error configuring root user");
            System.exit(1);
        }
        try {
            LOG.info("Starting StandaloneServer");
            StandaloneServer s = new StandaloneServer(conf);
            s.run();
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
