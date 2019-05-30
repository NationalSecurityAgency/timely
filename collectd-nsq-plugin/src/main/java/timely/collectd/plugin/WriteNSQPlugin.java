package timely.collectd.plugin;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.collectd.api.Collectd;
import org.collectd.api.CollectdConfigInterface;
import org.collectd.api.CollectdShutdownInterface;
import org.collectd.api.CollectdWriteInterface;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;

/**
 *
 * CollectD plugin that writes metrics collected by the StatsD plugin to NSQ.
 * NSQ can be configured to write statsd metrics to CollectD. If you do this,
 * set the statsd prefix in NSQ to 'nsq'.
 *
 */
public class WriteNSQPlugin extends CollectDPluginParent
        implements CollectdConfigInterface, CollectdShutdownInterface, CollectdWriteInterface {

    private String host = null;
    private int port = 0;
    private String topic = "metrics#ephemeral";
    private String endpoint = null;
    private GenericObjectPool<CloseableHttpClient> clientPool = null;

    // pool should be limited by the number of WriteThreads configured in
    // collectd
    final private int POOL_MAX_SIZE = Integer.MAX_VALUE;

    public WriteNSQPlugin() {
        Collectd.registerConfig(WriteNSQPlugin.class.getName(), this);
        Collectd.registerShutdown(WriteNSQPlugin.class.getName(), this);
        Collectd.registerWrite(WriteNSQPlugin.class.getName(), this);
    }

    @Override
    public int write(ValueList vl) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        super.process(vl, out);
        CloseableHttpClient client = null;
        try {
            out.flush();
            HttpPost post = new HttpPost(endpoint);
            EntityBuilder request = EntityBuilder.create();
            request.setBinary(baos.toByteArray());
            post.setEntity(request.build());
            client = clientPool.borrowObject();
            CloseableHttpResponse response = client.execute(post);
            try {
                int code = response.getStatusLine().getStatusCode();
                String msg = response.getStatusLine().getReasonPhrase();
                // Consume the entire response so the connection will be reused
                EntityUtils.consume(response.getEntity());
                if (code != HttpStatus.SC_OK) {
                    Collectd.logInfo("Code: " + code + ", msg: " + msg);
                }
            } finally {
                response.close();
            }
        } catch (Exception e) {
            Collectd.logError("Error sending metrics to NSQ: " + e.getMessage());
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e1) {
                }
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception e2) {
                }
            }
        } finally {
            if (client != null) {
                clientPool.returnObject(client);
            }
            if (null != out) {
                try {
                    out.close();
                } catch (IOException e) {
                }
            }
        }
        return 0;
    }

    @Override
    public void write(String metric, OutputStream out) {
        try {
            ((DataOutputStream) out).writeBytes(metric);
        } catch (IOException e) {
            Collectd.logError("Error writing metric: " + e.getMessage());
            try {
                out.close(); // Should cause IOException in write.
            } catch (IOException e1) {
            }
        }
    }

    @Override
    public int shutdown() {
        Collectd.logInfo("Shutting down connection to NSQ at " + host + ":" + port);
        clientPool.close();
        return 0;
    }

    @Override
    public int config(OConfigItem config) {
        super.config(config);
        GenericObjectPoolConfig<CloseableHttpClient> poolConfig = new GenericObjectPoolConfig<>();
        // use max size for maxTotal and maxIdle
        // no need to activate or passivate
        poolConfig.setMaxTotal(POOL_MAX_SIZE);
        poolConfig.setMaxIdle(POOL_MAX_SIZE);
        poolConfig.setTestOnReturn(true);
        PooledCloseableHttpClientFactory pooledCloseableHttpClientFactory = new PooledCloseableHttpClientFactory();
        pooledCloseableHttpClientFactory.config(config);

        for (OConfigItem child : config.getChildren()) {
            switch (child.getKey()) {
                case "host":
                case "hostname":
                case "Host":
                case "HostName":
                    String hostString = child.getValues().get(0).getString();
                    String[] hosts = hostString.split(",");
                    if (hosts.length == 1) {
                        host = hosts[0];
                    } else {
                        host = hosts[new Random().nextInt(hosts.length)];
                    }
                    break;
                case "Port":
                case "port":
                    port = Integer.parseInt(child.getValues().get(0).getString());
                    break;
                case "topic":
                case "Topic":
                    topic = child.getValues().get(0).getString();
                    break;
                default:
            }
        }
        if (host == null || port == 0 || topic == null) {
            Collectd.logError("NSQ host, port, and topic must be configured");
            return -1;
        }
        this.clientPool = new GenericObjectPool<>(pooledCloseableHttpClientFactory, poolConfig);
        this.endpoint = "http://" + host + ":" + port + "/mpub?topic=" + topic;
        return 0;
    }

}
