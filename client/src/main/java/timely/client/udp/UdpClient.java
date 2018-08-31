package timely.client.udp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

public class UdpClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(UdpClient.class);

    private final String host;
    private final int port;
    private final InetSocketAddress address;
    private final DatagramPacket packet;
    private DatagramSocket sock;

    public UdpClient(String host, int port) {
        this.address = new InetSocketAddress(host, port);
        this.packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), port);
        this.host = host;
        this.port = port;
    }

    public void open() throws IOException {
        if (null == sock) {
            this.sock = new DatagramSocket();
        }
    }

    public void write(String metric) throws IOException {
        if (null == this.sock) {
            throw new IllegalStateException("Must call open first");
        }
        this.packet.setData(metric.getBytes(UTF_8));
        LOG.info("writing '" + metric + "' to " + this.host + ":" + this.port);
        this.sock.send(packet);
    }

    public void flush() throws IOException {
    }

    public void close() throws IOException {
        if (null != sock) {
            sock.close();
            sock = null;
        }
    }

}
