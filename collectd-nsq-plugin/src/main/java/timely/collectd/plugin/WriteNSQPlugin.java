package timely.collectd.plugin;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
public class WriteNSQPlugin extends CollectDPluginParent implements CollectdConfigInterface, CollectdShutdownInterface,
        CollectdWriteInterface {

    private String host = null;
    private int port = 0;
    private String topic = "metrics#ephemeral";
    private String endpoint = null;
    private ByteArrayOutputStream baos = null;
    private DataOutputStream out = null;

    private CloseableHttpClient client = HttpClients.createDefault();

    public WriteNSQPlugin() {
        Collectd.registerConfig(WriteNSQPlugin.class.getName(), this);
        Collectd.registerShutdown(WriteNSQPlugin.class.getName(), this);
        Collectd.registerWrite(WriteNSQPlugin.class.getName(), this);
    }

    public synchronized int write(ValueList vl) {
        baos = new ByteArrayOutputStream();
        out = new DataOutputStream(baos);
        super.process(vl);
        try {
            out.flush();
            HttpPost post = new HttpPost(endpoint);
            EntityBuilder request = EntityBuilder.create();
            request.setBinary(baos.toByteArray());
            post.setEntity(request.build());
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
        } catch (IOException e) {
            Collectd.logError("Error sending metrics to NSQ: " + e.getMessage());
        } finally {
            if (null != out) {
                try {
                    out.close();
                    out = null;
                } catch (IOException e) {
                }
            }
        }
        return 0;
    }

    @Override
    public void write(String metric) {
        try {
            out.writeBytes(metric);
        } catch (IOException e) {
            Collectd.logError("Error writing metric: " + e.getMessage());
            try {
                out.close(); // Should cause IOException in write.
            } catch (IOException e1) {
            }
        }
    }

    public void flush() {
    }

    public int shutdown() {
        Collectd.logInfo("Shutting down connection to NSQ at " + host + ":" + port);
        try {
            client.close();
        } catch (IOException e) {
            Collectd.logError("Error closing HttpClient: " + e.getMessage());
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
        this.endpoint = "http://" + host + ":" + port + "/mpub?topic=" + topic;
        if (null != host) {
            return 0;
        }
        if (host == null) {
            Collectd.logError("NSQ host must be configured");
        }
        if (port == 0) {
            Collectd.logError("NSQ port must be configured");
        }
        return -1;
    }

}
