package timely.balancer.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.MetricRequest;
import timely.api.request.timeseries.HttpRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.http.HttpClientPool;
import timely.balancer.resolver.MetricResolver;
import timely.netty.http.TimelyHttpHandler;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HttpRelayHandler extends SimpleChannelInboundHandler<HttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRelayHandler.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";
    private static final String NO_AUTHORIZATIONS = "";

    private final BalancerConfiguration conf;
    private final HttpClientPool httpClientPool;
    private MetricResolver metricResolver;

    public HttpRelayHandler(BalancerConfiguration config, MetricResolver metricResolver, HttpClientPool httpClientPool) {
        this.conf = config;
        this.metricResolver = metricResolver;
        this.httpClientPool = httpClientPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {
        byte[] buf;
        HttpClient client = null;
        TimelyBalancedHost k = null;
        HttpResponse relayedResponse = null;
        try {
            FullHttpRequest request = msg.getHttpRequest();
            String originalURI = request.getUri();
            LOG.info("uri=" + originalURI);
            originalURI = encodeURI(originalURI);
            // originalURI = URLEncoder.encode(originalURI, "UTF-8");

            String metric = null;
            if (msg instanceof QueryRequest) {
                Collection<QueryRequest.SubQuery> subqueries = ((QueryRequest) msg).getQueries();
                if (subqueries != null) {
                    Iterator<QueryRequest.SubQuery> itr = subqueries.iterator();
                    if (itr.hasNext()) {
                        metric = itr.next().getMetric();
                    }
                }
            }
            if (msg instanceof MetricRequest) {
                metric = ((MetricRequest) msg).getMetric().getName();
            }
            k = metricResolver.getHostPortKey(metric);
            client = httpClientPool.borrowObject(k);

            List<Map.Entry<String, String>> headerList = request.headers().entries();
            Header[] headerArray = new Header[headerList.size()];
            for (int x = 0; x < headerArray.length; x++) {
                Map.Entry<String, String> h = headerList.get(x);
                headerArray[x] = new BasicHeader(h.getKey(), h.getValue());
            }

            String relayURI = "https://" + k.getHost() + ":" + k.getHttpPort() + originalURI;
            if (msg instanceof HttpGetRequest) {
                LOG.info("Get request");
                HttpUriRequest relayedRequest = new HttpGet(relayURI);
                if (headerArray.length > 0) {
                    relayedRequest.setHeaders(headerArray);
                }
                relayedResponse = client.execute(relayedRequest);

            } else if (msg instanceof HttpPostRequest) {
                LOG.info("Post request");
                HttpUriRequest relayedRequest = new HttpPost(relayURI);
                byte[] content = ((DefaultFullHttpRequest) request).content().array();
                ByteArrayEntity entity = new ByteArrayEntity(content);
                ((HttpPost) relayedRequest).setEntity(entity);
                if (headerArray.length > 0) {
                    relayedRequest.setHeaders(headerArray);
                }
                relayedResponse = client.execute(relayedRequest);

            }
        } catch (Exception e) {
            String message = e.getMessage();
            if (message == null) {
                LOG.error("", e);
            } else {
                if (message.contains("No matching tags")) {
                    LOG.trace(message);
                } else {
                    LOG.error(message, e);
                }
            }
            this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    e.getMessage(), e.getLocalizedMessage(), e));
            return;
        } finally {
            if (client != null && k != null) {
                httpClientPool.returnObject(k, client);
            }
        }
        FullHttpResponse response = null;
        if (relayedResponse == null) {
            this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(),
                    "Method not allowed", "Method not allowed"));
        } else {
            // buf = JsonUtil.getObjectMapper().writeValueAsBytes(msg);
            ByteArrayOutputStream baos = new MyByteArrayOutputStream();
            StreamUtils.copy(relayedResponse.getEntity().getContent(), baos);

            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(relayedResponse
                    .getStatusLine().getStatusCode()), Unpooled.copiedBuffer(baos.toByteArray()));
            for (Header h : relayedResponse.getAllHeaders()) {
                response.headers().add(h.getName(), h.getValue());
            }
            // response.headers().set(HttpHeaders.Names.CONTENT_TYPE,
            // Constants.JSON_TYPE);
            response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
            sendResponse(ctx, response);
        }

    }

    static public class MyByteArrayOutputStream extends ByteArrayOutputStream {

        public MyByteArrayOutputStream() {
        }

        public MyByteArrayOutputStream(int size) {
            super(size);
        }

        public int getCount() {
            return count;
        }

        public byte[] getBuf() {
            return buf;
        }
    }

    static public String encodeURI(String s) {
        try {
            s = s.replaceAll("\\{", URLEncoder.encode("{", "UTF-8"));
            s = s.replaceAll("\\}", URLEncoder.encode("}", "UTF-8"));
        } catch (Exception e) {

        }
        return s;
    }

    public static void main(String[] args) {

        String s = "https://127.0.0.1:4243/api/query?start=1356998400000&end=1528398304000&m=sum:rate{false,100,0}:sys.thermal.gauge{class=aws}";
        URI uri = URI.create(encodeURI(s));

        System.out.println(uri.toString());
    }

}
