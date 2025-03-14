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
    private long INIT_BACKOFF = 1000;
    private long MAX_BACKOFF = 60000;
    private long backoff = INIT_BACKOFF;
    private int bufferSize;
    private long latency;
    private int writesSinceFlush = 0;
    private long lastFlushTime = System.currentTimeMillis();

    public TcpClient(String hostname, int port) {
        this(hostname, port, -1);
    }

    public TcpClient(String hostname, int port, int bufferSize) {
        this(hostname, port, bufferSize, -1);
    }

    public TcpClient(String hostname, int port, int bufferSize, long latency) {
        this.host = hostname;
        this.port = port;
        this.bufferSize = bufferSize;
        this.latency = latency;
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
        try {
            out.write(metric);
            writesSinceFlush++;
            if (bufferSize > 0 && writesSinceFlush >= bufferSize) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Flushing buffer writesSinceFlush >= %d", writesSinceFlush));
                }
                flush();
            }
            if (latency > 0 && (System.currentTimeMillis() - lastFlushTime) >= latency) {
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Flushing buffer timeSinceFlush >= %d", latency));
                }
                flush();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            reset();
            throw e;
        }
    }

    public synchronized void flush() throws IOException {
        if (null != out) {
            try {
                long start = System.currentTimeMillis();
                out.flush();
                lastFlushTime = System.currentTimeMillis();
                if (log.isTraceEnabled()) {
                    log.trace(String.format("Flushed %d metrics to socket in %d ms", writesSinceFlush, (lastFlushTime - start)));
                }
                writesSinceFlush = 0;
            } catch (IOException e) {
                log.error(String.format("Failed to flush %d metrics to socket", writesSinceFlush), e);
                reset();
                throw e;
            }
        }
    }

    private synchronized void reset() {
        out = null;
        sock = null;
        backoff = INIT_BACKOFF;
        writesSinceFlush = 0;
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * Closes the tcp connection to Timely
     *
     * @throws IOException
     *             if an error occurs
     */
    @Override
    public synchronized void close() throws IOException {
        log.info(String.format("Shutting down connection to Timely at %s:%d", host, port));
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
                log.error(String.format("Error closing connection to Timely at %s:%d", host, port), e);
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
                    backoff = INIT_BACKOFF;
                    log.info(String.format("Connected to Timely at %s:%d", host, port));
                } catch (Exception e) {
                    log.error(String.format("Error connecting to Timely at %s:%d", host, port), e);
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
                    backoff = Math.min(MAX_BACKOFF, backoff * 2);
                    if (log.isTraceEnabled()) {
                        log.trace(String.format("Will retry connection in %d ms", backoff));
                    }
                    return -1;
                }
            } else {
                return -1;
            }
        }
        return 0;
    }

    public long getConnectTime() {
        return connectTime;
    }

    public int getWritesSinceFlush() {
        return writesSinceFlush;
    }

    public long getTimeSinceFlush() {
        return System.currentTimeMillis() - lastFlushTime;
    }

    public synchronized boolean isConnected() {
        boolean isConnected = (null != sock && sock.isConnected());
        if (isConnected) {
            return true;
        } else {
            int status = connect();
            return status == 0;
        }
    }
}
