package timely.balancer.netty.http;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;

import com.google.common.base.Throwables;
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
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpRequest;
import timely.api.request.MetricRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.util.HttpHeaderUtils;
import timely.auth.util.ProxiedEntityUtils;
import timely.balancer.MetricResolver;
import timely.balancer.configuration.BalancerHttpProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.http.HttpClientPool;
import timely.netty.http.TimelyHttpHandler;

public class HttpRelayHandler extends SimpleChannelInboundHandler<HttpRequest> implements TimelyHttpHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpRelayHandler.class);
    private BalancerHttpProperties balancerHttpProperties;
    private final HttpClientPool httpClientPool;
    private MetricResolver metricResolver;
    private ExecutorService requestExecutor = Executors.newCachedThreadPool();

    public HttpRelayHandler(BalancerHttpProperties balancerHttpProperties, MetricResolver metricResolver, HttpClientPool httpClientPool) {
        this.balancerHttpProperties = balancerHttpProperties;
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
            String relayURI = "";
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

                Multimap<String,String> headers = HttpHeaderUtils.toMultimap(request.headers());
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
                for (Map.Entry<String,String> h : headers.entries()) {
                    if (!h.getKey().equals(CONTENT_LENGTH.toString())) {
                        relayedHeaderList.add(new BasicHeader(h.getKey(), h.getValue()));
                    }
                }
                Header[] headerArray = new Header[relayedHeaderList.size()];
                relayedHeaderList.toArray(headerArray);
                relayURI = "https://" + k.getHost() + ":" + k.getHttpPort() + originalURI;
                HttpRequestBase relayedRequest = null;
                if (request.method().equals(HttpMethod.GET)) {
                    relayedRequest = new HttpGet(relayURI);
                } else if (request.method().equals(HttpMethod.POST)) {
                    relayedRequest = new HttpPost(relayURI);
                    String content = request.content().toString(StandardCharsets.UTF_8);
                    StringEntity entity = new StringEntity(content);
                    ((HttpPost) relayedRequest).setEntity(entity);
                } else {
                    throw new IllegalArgumentException("Unsupported HTTP method: " + request.method());
                }
                if (headerArray.length > 0) {
                    relayedRequest.setHeaders(headerArray);
                }
                relayedResponse = executeHttpRequest(client, relayedRequest, balancerHttpProperties.getRequestTimeout());
            } catch (IOException e) {
                String message = String.format("%s calling %s", e.getMessage() == null ? "" : e.getMessage(), relayURI);
                log.error(message, e);
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.GATEWAY_TIMEOUT.code(), e.getMessage(), e.getLocalizedMessage(), e));
                return;
            } catch (Exception e) {
                String message = e.getMessage();
                if (message != null && message.contains("No matching tags")) {
                    log.trace(message);
                } else {
                    message = String.format("%s calling %s", e.getMessage() == null ? "" : e.getMessage(), relayURI);
                    log.error(message, e);
                }
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), e.getMessage(), e.getLocalizedMessage(), e));
                return;
            }
            FullHttpResponse response;
            if (relayedResponse == null) {
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(), "Method not allowed", "Method not allowed"));
            } else {
                ByteArrayOutputStream baos = new MyByteArrayOutputStream();
                StreamUtils.copy(relayedResponse.getEntity().getContent(), baos);

                response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(relayedResponse.getStatusLine().getStatusCode()),
                                Unpooled.copiedBuffer(baos.toByteArray()));
                for (Header h : relayedResponse.getAllHeaders()) {
                    response.headers().add(h.getName(), h.getValue());
                }
                response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
                sendResponse(ctx, response);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (relayedResponse != null) {
                relayedResponse.close();
            }
            if (client != null && k != null) {
                httpClientPool.returnObject(k, client);
            } else {
                log.error("NOT RETURNING CONNECTION! " + msg.getHttpRequest().uri());
            }
        }
    }

    static public class MyByteArrayOutputStream extends ByteArrayOutputStream {

        public MyByteArrayOutputStream() {}

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

    private CloseableHttpResponse executeHttpRequest(CloseableHttpClient httpClient, HttpRequestBase relayedRequest, long timeout) throws Exception {
        try {
            RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
            requestConfigBuilder.setConnectionRequestTimeout(this.balancerHttpProperties.getConnectionRequestTimeout());
            requestConfigBuilder.setConnectTimeout(this.balancerHttpProperties.getConnectTimeout());
            requestConfigBuilder.setSocketTimeout(this.balancerHttpProperties.getSocketTimeout());
            RequestConfig requestConfig = requestConfigBuilder.build();
            relayedRequest.setConfig(requestConfig);
            Future<CloseableHttpResponse> f = requestExecutor.submit(() -> httpClient.execute(relayedRequest));
            return f.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Throwable cause = Throwables.getRootCause(e);
            throw new IOException(cause.getMessage(), cause);
        } catch (Exception e) {
            throw e;
        }
    }
}
