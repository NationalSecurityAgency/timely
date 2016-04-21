package timely.collectd.plugin;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

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
public class WriteTimelyPlugin extends CollectDPluginParent implements CollectdConfigInterface,
        CollectdShutdownInterface, CollectdWriteInterface {

    private String host = null;
    private int port = 0;
    private Socket sock = null;
    private PrintWriter out = null;
    private long connectTime = 0L;
    private long backoff = 2000;

    public WriteTimelyPlugin() {
        Collectd.registerConfig(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerShutdown(WriteTimelyPlugin.class.getName(), this);
        Collectd.registerWrite(WriteTimelyPlugin.class.getName(), this);
    }

    private synchronized int connect() {
        if (null == sock || !sock.isConnected() || out.checkError()) {
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                try {
                    connectTime = System.currentTimeMillis();
                    sock = new Socket(host, port);
                    out = new PrintWriter(sock.getOutputStream(), false);
                    backoff = 2000;
                    Collectd.logInfo("Connected to Timely at " + host + ":" + port);
                } catch (IOException e) {
                    Collectd.logError("Error connecting to Timely at " + host + ":" + port + ". Error: "
                            + e.getMessage());
                    backoff = backoff * 2;
                    sock = null;
                    out = null;
                    Collectd.logWarning("Will retry connection in " + backoff + " ms.");
                    return -1;
                }
            } else {
                Collectd.logWarning("Not writing to Timely, waiting to reconnect");
                return 0;
            }
        }
        return 0;
    }

    public synchronized int write(ValueList vl) {

        int c = connect();
        if (c != 0) {
            return c;
        }
        super.process(vl);
        return 0;
    }

    @Override
    public void write(String metric) {
        out.write(metric);
    }

    public void flush() {
        if (null != out) {
            out.flush();
        }
    }

    public int shutdown() {
        Collectd.logInfo("Shutting down connection to Timely at " + host + ":" + port);
        if (null != sock) {
            try {
                if (null != out) {
                    out.close();
                }
                sock.close();
            } catch (IOException e) {
                Collectd.logError("Error closing connection to Timely at " + host + ":" + port + ". Error: "
                        + e.getMessage());
                return -1;
            }
        }
        return 0;
    }

    public int config(OConfigItem config) {
        super.config(config);
        for (OConfigItem child : config.getChildren()) {
            switch (child.getKey()) {
                case "host":
                case "hostname":
                case "Host":
                case "HostName":
                    host = child.getValues().get(0).getString();
                    break;
                case "Port":
                case "port":
                    port = Integer.parseInt(child.getValues().get(0).getString());
                    break;
                default:

            }
        }
        if (null != host) {
            return 0;
        }
        if (host == null) {
            Collectd.logError("Timely host must be configured");
        }
        if (port == 0) {
            Collectd.logError("Timely port must be configured");
        }
        return -1;
    }

}
