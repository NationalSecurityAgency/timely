package timely.collectd.plugin;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.collectd.api.Collectd;
import org.collectd.api.OConfigItem;

import java.net.Socket;

public class PooledSocketFactory implements PooledObjectFactory {

    private String host = null;
    private int port = 0;
    private static final long initialBackoff = 2000;

    public int config(OConfigItem config) {
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

    private Socket connect() {

        Socket socket = null;
        long currentBackoff = initialBackoff;
        while (socket == null) {
            try {
                socket = new Socket(host, port);
                Collectd.logInfo("Connected to Timely at " + host + ":" + port + " from local port:"
                        + socket.getLocalPort());
            } catch (Exception e) {
                Collectd.logError("Error connecting to Timely at " + host + ":" + port + ". Error: " + e.getMessage()
                        + ".  Will retry connection in " + currentBackoff + " ms.");
                try {
                    Thread.sleep(currentBackoff);
                } catch (InterruptedException e1) {

                }
                // max out reconnect period at one minute
                currentBackoff = (currentBackoff * 2 > 60000) ? 60000 : currentBackoff * 2;
            }
        }
        return socket;
    }

    @Override
    public PooledObject makeObject() throws Exception {
        return new DefaultPooledObject(connect());
    }

    @Override
    public void destroyObject(PooledObject pooledObject) throws Exception {
        try {
            Collectd.logInfo("Shutting down connection to Timely at " + host + ":" + port);
            Socket socket = (Socket) pooledObject.getObject();
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            Collectd.logError("Error closing connection to Timely at " + host + ":" + port + ". Error: "
                    + e.getMessage());
        } finally {
            pooledObject.invalidate();
            ;
        }
    }

    @Override
    public boolean validateObject(PooledObject pooledObject) {
        Socket socket = (Socket) pooledObject.getObject();
        return socket != null && !socket.isClosed();
    }

    @Override
    public void activateObject(PooledObject pooledObject) throws Exception {
        // do nothing
    }

    @Override
    public void passivateObject(PooledObject pooledObject) throws Exception {
        // do nothing
    }
}
