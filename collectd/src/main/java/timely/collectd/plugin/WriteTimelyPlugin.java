package timely.collectd.plugin;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.CollectdWriteInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.OConfigValue;
import org.collectd.api.ValueList;
import org.slf4j.event.Level;

/**
 *
 * CollectD plugin that writes metrics collected by the StatsD plugin to Timely.
 *
 */
public class WriteTimelyPlugin extends CollectDPluginParent implements CollectdConfigInterface, CollectdShutdownInterface, CollectdWriteInterface {

    private GenericObjectPool<Socket> socketPool = null;

    // pool should be limited by the number of WriteThreads configured in
    // collectd
    static final private int POOL_MAX_SIZE = Integer.MAX_VALUE;

    public WriteTimelyPlugin() {
        Collectd.registerConfig(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerShutdown(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerWrite(WriteTimelyPlugin.class.getName(), this);
    }

    @Override
    public void write(MetricData metricData) {
        Socket socket = null;
        try {
            socket = socketPool.borrowObject();
            if (socket == null) {
                return;
            }
            super.process(metricData, socket.getOutputStream());
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
        } finally {
            if (socket != null) {
                socketPool.returnObject(socket);
            }
        }
    }

    public int write(ValueList vl) {
        MetricData metricData = new MetricData(vl);
        write(metricData);
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
        Map<String,Object> configMap = new HashMap<>();
        for (OConfigItem c : config.getChildren()) {
            String key = c.getKey();
            OConfigValue value = c.getValues().get(0);
            int type = value.getType();
            if (type == 0) {
                configMap.put(key, value.getString());
            } else if (type == 1) {
                configMap.put(key, value.getNumber());
            } else if (type == 2) {
                configMap.put(key, value.getBoolean());
            }
        }

        int retval;
        PooledSocketFactory socketFactory = new PooledSocketFactory();
        retval = socketFactory.config(configMap);
        if (retval == 0) {
            GenericObjectPoolConfig<Socket> poolConfig = new GenericObjectPoolConfig<>();
            // use max size for maxTotal and maxIdle
            // no need to activate and passivate
            poolConfig.setMaxTotal(POOL_MAX_SIZE);
            poolConfig.setMaxIdle(POOL_MAX_SIZE);
            poolConfig.setTestOnReturn(true);
            socketPool = new GenericObjectPool<>(socketFactory, poolConfig);
        }
        super.config(configMap);
        return retval;
    }

    public void log(Level level, String s) {
        // collectd must be compiled with debug enabled in order for log level debug to work
        if (isDebug()) {
            Collectd.logInfo(s);
        } else {
            if (level.equals(Level.DEBUG)) {
                Collectd.logDebug(s);
            } else if (level.equals(Level.INFO)) {
                Collectd.logInfo(s);
            } else if (level.equals(Level.WARN)) {
                Collectd.logWarning(s);
            } else if (level.equals(Level.ERROR)) {
                Collectd.logError(s);
            }
        }
    }
}
