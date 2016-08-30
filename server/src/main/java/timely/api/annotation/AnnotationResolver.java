package timely.api.annotation;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.TcpRequest;
import timely.api.request.UdpRequest;
import timely.api.request.WebSocketRequest;

public class AnnotationResolver {

    private static final Logger LOG = LoggerFactory.getLogger(AnnotationResolver.class);
    private static final FastClasspathScanner scanner = new FastClasspathScanner("timely.api").scan();
    private static List<String> tcpClassNames;
    private static List<String> httpClassNames;
    private static List<String> wsClassNames;
    private static List<String> udpClassNames;
    private static List<Class<?>> tcpClasses = new ArrayList<>();
    private static List<Class<?>> httpClasses = new ArrayList<>();
    private static List<Class<?>> wsClasses = new ArrayList<>();
    private static List<Class<?>> udpClasses = new ArrayList<>();

    private AnnotationResolver() {
    }

    static {
        tcpClassNames = scanner.getNamesOfClassesWithAnnotation(Tcp.class);
        LOG.trace("Found tcp classes: {}", tcpClassNames);
        if (null != tcpClassNames) {
            for (String cls : tcpClassNames) {
                try {
                    tcpClasses.add(Class.forName(cls));
                } catch (Exception e) {
                    LOG.error("Error loading/creating class: " + cls, e);
                }
            }
        }
        LOG.trace("Loaded tcp classes: {}", tcpClasses);
        httpClassNames = scanner.getNamesOfClassesWithAnnotation(Http.class);
        LOG.trace("Found http class names: {}", httpClassNames);
        if (null != httpClassNames) {
            for (String cls : httpClassNames) {
                try {
                    httpClasses.add(Class.forName(cls));
                } catch (Exception e) {
                    LOG.error("Error loading/creating class: " + cls, e);
                }
            }
        }
        LOG.trace("Loaded http classes: {}", httpClasses);
        wsClassNames = scanner.getNamesOfClassesWithAnnotation(WebSocket.class);
        LOG.trace("Found web socket class names: {}", wsClassNames);
        if (null != wsClassNames) {
            for (String cls : wsClassNames) {
                try {
                    wsClasses.add(Class.forName(cls));
                } catch (Exception e) {
                    LOG.error("Error loading/creating class: " + cls, e);
                }
            }
        }
        LOG.trace("Loaded web socket classes: {}", wsClasses);
        udpClassNames = scanner.getNamesOfClassesWithAnnotation(Udp.class);
        LOG.trace("Found udp classes: {}", udpClassNames);
        if (null != udpClassNames) {
            for (String cls : udpClassNames) {
                try {
                    udpClasses.add(Class.forName(cls));
                } catch (Exception e) {
                    LOG.error("Error loading/creating class: " + cls, e);
                }
            }
        }
        LOG.trace("Loaded udp classes: {}", tcpClasses);
    }

    public static List<Class<?>> getTcpClasses() {
        return tcpClasses;
    }

    public static TcpRequest getClassForTcpOperation(String operation) throws Exception {
        for (Class<?> c : tcpClasses) {
            if (c.getAnnotation(Tcp.class).operation().equals(operation)) {
                Object o = c.newInstance();
                if (o instanceof TcpRequest) {
                    LOG.trace("Returning {} for TCP operation {}", c, operation);
                    return (TcpRequest) o;
                }
            }
        }
        return null;
    }

    public static HttpGetRequest getClassForHttpGet(String path) throws Exception {
        LOG.trace("Looking for class that support http get at path: {}", path);
        for (Class<?> c : httpClasses) {
            Http http = c.getAnnotation(Http.class);
            if (http.path().equals(path) && (HttpGetRequest.class.isAssignableFrom(c))) {
                Object o = c.newInstance();
                LOG.trace("Returning: {}", c.getName());
                return (HttpGetRequest) o;
            }
        }
        return null;
    }

    public static HttpPostRequest getClassForHttpPost(String path) throws Exception {
        LOG.trace("Looking for class that support http post at path: {}", path);
        for (Class<?> c : httpClasses) {
            Http http = c.getAnnotation(Http.class);
            if (http.path().equals(path) && (HttpPostRequest.class.isAssignableFrom(c))) {
                Object o = c.newInstance();
                LOG.trace("Returning: {}", c.getName());
                return (HttpPostRequest) o;
            }
        }
        return null;
    }

    public static List<Class<?>> getWebSocketClasses() {
        return wsClasses;
    }

    public static WebSocketRequest getClassForWebSocketOperation(String operation) throws Exception {
        for (Class<?> c : wsClasses) {
            if (c.getAnnotation(WebSocket.class).operation().equals(operation)) {
                Object o = c.newInstance();
                if (o instanceof WebSocketRequest) {
                    LOG.trace("Returning {} for WebSocket operation {}", c, operation);
                    return (WebSocketRequest) o;
                }
            }
        }
        return null;
    }

    public static UdpRequest getClassForUdpOperation(String operation) throws Exception {
        for (Class<?> c : udpClasses) {
            if (c.getAnnotation(Udp.class).operation().equals(operation)) {
                Object o = c.newInstance();
                if (o instanceof UdpRequest) {
                    LOG.trace("Returning {} for UDP operation {}", c, operation);
                    return (UdpRequest) o;
                }
            }
        }
        return null;
    }

}
