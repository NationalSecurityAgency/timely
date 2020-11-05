package timely.balancer.netty.http;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.MetricRequest;
import timely.api.request.timeseries.HttpRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.auth.util.HttpHeaderUtils;
import timely.auth.util.ProxiedEntityUtils;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.http.HttpClientPool;
import timely.balancer.resolver.MetricResolver;
import timely.netty.http.TimelyHttpHandler;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class HttpRelayHandler extends SimpleChannelInboundHandler<HttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRelayHandler.class);
    private final HttpClientPool httpClientPool;
    private MetricResolver metricResolver;

    public HttpRelayHandler(MetricResolver metricResolver, HttpClientPool httpClientPool) {
        this.metricResolver = metricResolver;
        this.httpClientPool = httpClientPool;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {

        CloseableHttpClient client = null;
        TimelyBalancedHost k = null;
        CloseableHttpResponse relayedResponse = null;
        FullHttpRequest request = null;
        try {
            try {
                request = msg.getHttpRequest();
                String originalURI = request.uri();
                originalURI = encodeURI(originalURI);

                String metric = null;
                if (msg instanceof QueryRequest) {
                    Collection<QueryRequest.SubQuery> subqueries = ((QueryRequest) msg).getQueries();
                    if (subqueries != null) {
                        Iterator<QueryRequest.SubQuery> itr = subqueries.iterator();
                        if (itr.hasNext()) {
                            metric = itr.next().getMetric();
                        }
                    }
                    k = metricResolver.getHostPortKey(metric);
                } else if (msg instanceof MetricRequest) {
                    metric = ((MetricRequest) msg).getMetric().getName();
                    k = metricResolver.getHostPortKeyIngest(metric);
                } else {
                    k = metricResolver.getHostPortKey(null);
                }
                client = httpClientPool.borrowObject(k);

                Multimap<String, String> headers = HttpHeaderUtils.toMultimap(request.headers());
                if (msg instanceof AuthenticatedRequest) {
                    AuthenticatedRequest authenticatedRequest = (AuthenticatedRequest) msg;
                    TimelyAuthenticationToken token = authenticatedRequest.getToken();
                    if (token != null) {
                        if (token.getClientCert() != null) {
                            ProxiedEntityUtils.addProxyHeaders(headers, token.getClientCert());
                        }
                    }
                }

                List<Header> relayedHeaderList = new ArrayList<>();
                for (Map.Entry<String, String> h : headers.entries()) {
                    if (!h.getKey().equals(CONTENT_LENGTH.toString())) {
                        relayedHeaderList.add(new BasicHeader(h.getKey(), h.getValue()));
                    }
                }
                Header[] headerArray = new Header[relayedHeaderList.size()];
                relayedHeaderList.toArray(headerArray);

                String relayURI = "https://" + k.getHost() + ":" + k.getHttpPort() + originalURI;
                if (request.method().equals(HttpMethod.GET)) {
                    HttpUriRequest relayedRequest = new HttpGet(relayURI);
                    if (headerArray.length > 0) {
                        relayedRequest.setHeaders(headerArray);
                    }
                    relayedResponse = client.execute(relayedRequest);

                } else if (request.method().equals(HttpMethod.POST)) {
                    HttpPost relayedRequest = new HttpPost(relayURI);

                    String content = request.content().toString(StandardCharsets.UTF_8);
                    StringEntity entity = new StringEntity(content);
                    relayedRequest.setEntity(entity);
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
                if (request != null) {
                    ReferenceCountUtil.release(request);
                }
            }
            FullHttpResponse response = null;
            if (relayedResponse == null) {
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(),
                        "Method not allowed", "Method not allowed"));
            } else {
                ByteArrayOutputStream baos = new MyByteArrayOutputStream();
                StreamUtils.copy(relayedResponse.getEntity().getContent(), baos);

                response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                        HttpResponseStatus.valueOf(relayedResponse.getStatusLine().getStatusCode()),
                        Unpooled.copiedBuffer(baos.toByteArray()));
                for (Header h : relayedResponse.getAllHeaders()) {
                    response.headers().add(h.getName(), h.getValue());
                }
                response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
                sendResponse(ctx, response);
            }
        } finally {
            if (relayedResponse != null) {
                relayedResponse.close();
            }
            if (client != null && k != null) {
                httpClientPool.returnObject(k, client);
            } else {
                LOG.error("NOT RETURNING CONNECTION! " + msg.getHttpRequest().uri());
            }
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
}
