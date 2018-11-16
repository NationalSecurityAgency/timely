package timely.collectd.plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.CollectdWriteInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

/**
 *
 * CollectD plugin that writes metrics collected by the StatsD plugin to Timely.
 *
 */
public class WriteTimelyPlugin extends CollectDPluginParent
        implements CollectdConfigInterface, CollectdShutdownInterface, CollectdWriteInterface {

    private GenericObjectPool<Socket> socketPool = null;

    // pool should be limited by the number of WriteThreads configured in
    // collectd
    final private int POOL_MAX_SIZE = Integer.MAX_VALUE;

    public WriteTimelyPlugin() {
        Collectd.registerConfig(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerShutdown(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerWrite(WriteTimelyPlugin.class.getName(), this);
    }

    public int write(ValueList vl) {

        Socket socket = null;
        try {
            socket = socketPool.borrowObject();
            if (socket == null) {
                return -1;
            }
            super.process(vl, socket.getOutputStream());
        } catch (Exception e) {
            Collectd.logWarning(e.getMessage());
            try {
                // close socket so that the object pool will discard it and
                // reconnect
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e1) {
                Collectd.logError(e1.getMessage());
            }
            return -1;
        } finally {
            if (socket != null) {
                socketPool.returnObject(socket);
            }
        }
        return 0;
    }

    @Override
    public void write(String metric, OutputStream out) {
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), false);
        printWriter.write(metric);
        printWriter.flush();
        if (printWriter.checkError()) {
            throw new RuntimeException("Error writing to Timely");
        }
    }

    public int shutdown() {
        socketPool.close();
        return 0;
    }

    public int config(OConfigItem config) {
        super.config(config);
        int retval = 0;
        PooledSocketFactory socketFactory = new PooledSocketFactory();
        retval = socketFactory.config(config);
        if (retval == 0) {
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            // use max size for maxTotal and maxIdle
            // no need to activate and passivate
            poolConfig.setMaxTotal(POOL_MAX_SIZE);
            poolConfig.setMaxIdle(POOL_MAX_SIZE);
            poolConfig.setTestOnReturn(true);
            socketPool = new GenericObjectPool(socketFactory, poolConfig);
        }
        return retval;
    }

}
