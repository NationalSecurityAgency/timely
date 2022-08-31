package timely.client.tcp;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private final String host;
    private final int port;
    private Socket sock = null;
    private BufferedWriter out = null;
    private long connectTime = 0L;
    private long backoff = 2000;
    private int bufferSize = -1;
    private int writesSinceFlush = 0;

    public TcpClient(String hostname, int port) {
        this(hostname, port, -1);
    }

    public TcpClient(String hostname, int port, int bufferSize) {
        this.host = hostname;
        this.port = port;
        this.bufferSize = bufferSize;
    }

    /**
     * Opens a TCP connection to the specified host and port
     *
     * @throws IOException
     *             if an error occurs
     */
    public void open() throws IOException {
        if (connect() != 0) {
            throw new IOException();
        }
    }

    /**
     * Write a metric to Timely
     *
     * @param metric
     *            newline terminated string representation of Timely metric
     */
    public synchronized void write(String metric) throws IOException {
        if (connect() != 0) {
            throw new IOException();
        }
        out.write(metric);
        writesSinceFlush++;
        if (bufferSize > 0 && writesSinceFlush >= bufferSize) {
            flush();
        }
    }

    public synchronized void flush() throws IOException {
        if (null != out) {
            out.flush();
            writesSinceFlush = 0;
        }
    }

    /**
     * Closes the tcp connection to Timely
     *
     * @throws IOException
     *             if an error occurs
     */
    @Override
    public void close() throws IOException {
        log.trace("Shutting down connection to Timely at {}:{}", host, port);
        if (null != sock) {
            try {
                if (null != out) {
                    if (bufferSize > 0) {
                        flush();
                    }
                    out.close();
                }
                sock.close();
            } catch (IOException e) {
                log.error("Error closing connection to Timely at " + host + ":" + port + ". Error: " + e.getMessage());
            } finally {
                sock = null;
            }
        }
    }

    private synchronized int connect() {
        if (null == sock || !sock.isConnected()) {
            if (System.currentTimeMillis() > (connectTime + backoff)) {
                OutputStreamWriter osw = null;
                try {
                    connectTime = System.currentTimeMillis();
                    sock = new Socket(host, port);
                    osw = new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8);
                    out = new BufferedWriter(osw);
                    backoff = 2000;
                    log.trace("Connected to Timely at {}:{}", host, port);
                } catch (Exception e) {
                    log.error("Error connecting to Timely at {}:{} - {}", host, port, e.getMessage());
                    if (sock != null) {
                        try {
                            sock.close();
                        } catch (IOException e1) {
                            log.error(e1.getMessage());
                        } finally {
                            sock = null;
                        }
                    }
                    if (osw != null) {
                        try {
                            osw.close();
                        } catch (IOException e1) {
                            log.error(e1.getMessage());
                        }
                    }
                    if (out != null) {
                        try {
                            out.close();
                        } catch (IOException e1) {
                            log.error(e1.getMessage());
                        } finally {
                            out = null;
                        }
                    }
                    backoff = backoff * 2;
                    log.info("Will retry connection in {} ms.", backoff);
                    return -1;
                }
            } else {
                log.warn("Not writing to Timely, waiting to reconnect");
                return -1;
            }
        }
        return 0;
    }

    public long getConnectTime() {
        return connectTime;
    }
}
