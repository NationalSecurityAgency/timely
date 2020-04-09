package timely.grafana.auth.netty.http;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;

import java.io.ByteArrayOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import timely.api.request.timeseries.HttpRequest;
import timely.api.response.TimelyException;
import timely.auth.TimelyPrincipal;
import timely.auth.TimelyUser;
import timely.auth.util.DnUtils;
import timely.auth.util.HttpHeaderUtils;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.http.HttpClientPool;
import timely.grafana.auth.configuration.GrafanaAuthConfiguration;
import timely.grafana.request.GrafanaRequest;
import timely.netty.http.TimelyHttpHandler;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class GrafanaRelayHandler extends SimpleChannelInboundHandler<HttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GrafanaRelayHandler.class);
    private final HttpClientPool httpClientPool;
    private GrafanaAuthConfiguration config;

    public GrafanaRelayHandler(GrafanaAuthConfiguration config, HttpClientPool httpClientPool) {
        this.config = config;
        this.httpClientPool = httpClientPool;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpRequest msg) throws Exception {

        CloseableHttpClient client = null;
        CloseableHttpResponse relayedResponse = null;
        FullHttpRequest request = null;
        GrafanaRequest grafanaRequest;
        TimelyBalancedHost k = null;
        String relayURI = null;
        try {
            try {
                request = msg.getHttpRequest();
                String originalURI = request.uri();
                originalURI = encodeURI(originalURI);

                if (msg instanceof GrafanaRequest) {
                    grafanaRequest = (GrafanaRequest) msg;
                } else {
                    throw new UnsupportedOperationException("Request not supported");
                }
                TimelyAuthenticationToken token = grafanaRequest.getToken();
                if (token == null) {
                    throw new IllegalStateException("No token on grafana request");
                }

                String protocol = config.getGrafana().getProtocol();
                String host = config.getGrafana().getHost();
                Integer port = config.getGrafana().getPort();
                k = TimelyBalancedHost.of(host, -1, port, -1, -1, null);
                client = httpClientPool.borrowObject(k);
                Multimap<String, String> headers = HttpHeaderUtils.toMultimap(request.headers());

                // If incoming user sets these headers, do not relay them
                headers.removeAll("x-webauth-user");
                headers.removeAll("x-webauth-name");
                TimelyPrincipal principal = token.getTimelyPrincipal();
                TimelyUser user = principal.getPrimaryUser();
                String subjectDn = user.getDn().subjectDN();
                String userName = DnUtils.getCommonName(subjectDn);
                String loginName = DnUtils.getShortName(subjectDn);
                headers.put("x-webauth-user", loginName);
                headers.put("x-webauth-name", userName);
                List<Header> relayedHeaderList = new ArrayList<>();
                for (Map.Entry<String, String> h : headers.entries()) {
                    if (!h.getKey().equals(CONTENT_LENGTH.toString())) {
                        relayedHeaderList.add(new BasicHeader(h.getKey(), h.getValue()));
                    }
                }
                Header[] headerArray = new Header[relayedHeaderList.size()];
                relayedHeaderList.toArray(headerArray);
                relayURI = protocol + "://" + host + ":" + port + originalURI;
                if (request.method().equals(HttpMethod.HEAD)) {
                    HttpHead relayedRequest = new HttpHead(relayURI);
                    if (headerArray.length > 0) {
                        relayedRequest.setHeaders(headerArray);
                    }
                    relayedResponse = client.execute(relayedRequest);
                } else if (request.method().equals(HttpMethod.GET)) {
                    HttpGet relayedRequest = new HttpGet(relayURI);
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
                } else if (request.method().equals(HttpMethod.PUT)) {
                    HttpPut relayedRequest = new HttpPut(relayURI);
                    String content = request.content().toString(StandardCharsets.UTF_8);
                    StringEntity entity = new StringEntity(content);
                    relayedRequest.setEntity(entity);
                    if (headerArray.length > 0) {
                        relayedRequest.setHeaders(headerArray);
                    }
                    relayedResponse = client.execute(relayedRequest);
                } else if (request.method().equals(HttpMethod.DELETE)) {
                    HttpDelete relayedRequest = new HttpDelete(relayURI);
                    if (headerArray.length > 0) {
                        relayedRequest.setHeaders(headerArray);
                    }
                    relayedResponse = client.execute(relayedRequest);
                } else if (request.method().equals(HttpMethod.OPTIONS)) {
                    HttpOptions relayedRequest = new HttpOptions(relayURI);
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
                    LOG.error(message, e);
                }
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        e.getMessage(), e.getLocalizedMessage(), e));
                return;
            } finally {
                if (request != null && request instanceof AutoCloseable) {
                    ReferenceCountUtil.release(request);
                }
            }

            FullHttpResponse response = null;
            if (relayedResponse == null) {
                this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(),
                        "Method not allowed", "Method not allowed"));
            } else {
                LOG.debug("RequestURL:{} ResponseCode:{}", relayURI, relayedResponse.getStatusLine().getStatusCode());
                HttpEntity httpEntity = relayedResponse.getEntity();
                if (httpEntity == null) {
                    response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                            HttpResponseStatus.valueOf(relayedResponse.getStatusLine().getStatusCode()));
                } else {
                    ByteArrayOutputStream baos = new MyByteArrayOutputStream();
                    StreamUtils.copy(httpEntity.getContent(), baos);
                    response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                            HttpResponseStatus.valueOf(relayedResponse.getStatusLine().getStatusCode()),
                            Unpooled.copiedBuffer(baos.toByteArray()));
                    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
                }
                for (Header h : relayedResponse.getAllHeaders()) {
                    response.headers().add(h.getName(), h.getValue());
                }
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
