package timely.collectd.plugin;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.collectd.api.Collectd;
import org.collectd.api.OConfigItem;

import java.io.IOException;
import java.net.Socket;

public class PooledSocketFactory implements PooledObjectFactory {

    private String host = null;
    private int port = 0;
    private static final long initialBackoff = 2000;
    private long currentBackoff = initialBackoff;

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
        if (null == socket || !socket.isConnected()) {
            long backoff = currentBackoff;
            long connectTime = 0L;
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                try {
                    socket = new Socket(host, port);
                    currentBackoff = initialBackoff;
                    Collectd.logInfo("Connected to Timely at " + host + ":" + port + " from local port:"
                            + socket.getLocalPort());
                } catch (IOException e) {
                    Collectd.logError("Error connecting to Timely at " + host + ":" + port + ". Error: "
                            + e.getMessage());
                    currentBackoff = backoff * 2;
                    Collectd.logWarning("Will retry connection in " + currentBackoff + " ms.");
                    return null;
                }
            } else {
                Collectd.logWarning("Not writing to Timely, waiting to reconnect");
                return null;
            }
        }
        return socket;
    }

    @Override
    public PooledObject makeObject() throws Exception {
        Socket socket = connect();
        return new DefaultPooledObject(socket);
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
        return pooledObject.getObject() != null;
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
