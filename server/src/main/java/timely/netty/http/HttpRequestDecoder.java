package timely.netty.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.annotation.AnnotationResolver;
import timely.api.query.response.StrictTransportResponse;
import timely.api.query.response.TimelyException;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.Request;
import timely.auth.AuthCache;
import timely.netty.Constants;

public class HttpRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestDecoder.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";
    // private static final String API_AGG = "/api/aggregators";
    // private static final String API_QUERY = "/api/query";
    // private static final String API_SEARCH_LOOKUP = "/api/search/lookup";
    // private static final String API_SUGGEST = "/api/suggest";
    // private static final String API_METRICS = "/api/metrics";
    // private static final String API_LOGIN = "/login";
    private static final String NO_AUTHORIZATIONS = "";

    private final Configuration conf;
    private boolean anonymousAccessAllowed = false;
    private final String nonSecureRedirectAddress;

    public HttpRequestDecoder(Configuration config) {
        this.conf = config;
        this.anonymousAccessAllowed = conf.getBoolean(Configuration.ALLOW_ANONYMOUS_ACCESS);
        this.nonSecureRedirectAddress = conf.get(Configuration.NON_SECURE_REDIRECT_PATH);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {

        LOG.trace(LOG_RECEIVED_REQUEST, msg);

        final String uri = msg.getUri();
        final QueryStringDecoder decoder = new QueryStringDecoder(uri);
        if (decoder.path().equals(nonSecureRedirectAddress)) {
            out.add(new StrictTransportResponse());
            return;
        }

        final StringBuilder buf = new StringBuilder();
        msg.headers().getAll(Names.COOKIE).forEach(h -> {
            ServerCookieDecoder.STRICT.decode(h).forEach(c -> {
                if (c.name().equals(Constants.COOKIE_NAME)) {
                    if (buf.length() == 0) {
                        buf.append(c.value());
                    }
                }
            });
        });

        String sId = buf.toString();
        if (sId.length() == 0 && this.anonymousAccessAllowed) {
            sId = NO_AUTHORIZATIONS;
        } else if (sId.length() == 0) {
            sId = null;
        }
        final String sessionId = sId;
        LOG.trace("SessionID: " + sessionId);

        final Request request;
        try {
            if (msg.getMethod().equals(HttpMethod.GET)) {
                HttpGetRequest get = AnnotationResolver.getClassForHttpGet(decoder.path());
                request = get.parseQueryParameters(decoder);
            } else if (msg.getMethod().equals(HttpMethod.POST)) {
                HttpPostRequest post = AnnotationResolver.getClassForHttpPost(decoder.path());
                String content = new String();
                ByteBuf body = msg.content();
                if (null != body) {
                	content = body.toString(StandardCharsets.UTF_8);
                }
                request = post.parseBody(content);
            } else {
                TimelyException e = new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(),
                        "unhandled method type", "");
                e.addResponseHeader(Names.ALLOW, HttpMethod.GET.name() + "," + HttpMethod.POST.name());
                LOG.warn("Unhandled HTTP request type {}", msg.getMethod());
                throw e;
            }
            if (request instanceof AuthenticatedRequest && sessionId != null) {
                ((AuthenticatedRequest) request).setSessionId(sessionId);
                ((AuthenticatedRequest) request).addHeaders(msg.headers().entries());
            }
            LOG.trace(LOG_PARSED_REQUEST, request);
            request.validate();
            try {
                AuthCache.enforceAccess(conf, request);
            } catch (Exception e) {
                out.clear();
                throw e;
            }
            out.add(request);
        } catch (UnsupportedOperationException e) {
            // Return the original http request to route to the static file
            // server
            msg.retain();
            out.add(msg);
        }

    }

    // public Collection<Request> parsePOST(String uri, String content) throws
    // JsonParseException, JsonMappingException,
    // IOException, TimelyException {
    // LOG.trace("Recevied POST content: {}", content);
    // final List<Request> requests = new ArrayList<>();
    // final QueryStringDecoder decoder = new QueryStringDecoder(uri);
    // if (decoder.path().equals(API_AGG)) {
    // // no Content
    // requests.add(new AggregatorsRequest());
    // } else if (decoder.path().equals(API_QUERY)) {
    // requests.add(JsonUtil.getObjectMapper().readValue(content,
    // QueryRequest.class));
    // } else if (decoder.path().equals(API_SEARCH_LOOKUP)) {
    // requests.add(JsonUtil.getObjectMapper().readValue(content,
    // SearchLookupRequest.class));
    // } else if (decoder.path().equals(API_SUGGEST)) {
    // requests.add(JsonUtil.getObjectMapper().readValue(content,
    // SuggestRequest.class));
    // } else if (decoder.path().equals(API_LOGIN)) {
    // requests.add(JsonUtil.getObjectMapper().readValue(content,
    // BasicAuthLoginRequest.class));
    // } else {
    // throw new UnsupportedOperationException();
    // }
    // LOG.trace("Parsed POST request {}", requests);
    // return requests;
    // }

    // public Collection<Request> parseURI(String uri) throws TimelyException {
    // final List<Request> requests = new ArrayList<>();
    // final QueryStringDecoder decoder = new QueryStringDecoder(uri);
    // if (decoder.path().equals(API_AGG)) {
    // requests.add(new AggregatorsRequest());
    // } else if (decoder.path().equals(API_QUERY)) {
    // final QueryRequest query = new QueryRequest();
    // query.setStart(Long.parseLong(decoder.parameters().get("start").get(0)));
    // if (decoder.parameters().containsKey("end")) {
    // query.setEnd(Long.parseLong(decoder.parameters().get("end").get(0)));
    // }
    // if (decoder.parameters().containsKey("m")) {
    // decoder.parameters()
    // .get("m")
    // .forEach(m -> {
    // final SubQuery sub = new SubQuery();
    // final String[] mParts = m.split(":");
    // if (mParts.length < 2) {
    // throw new
    // IllegalArgumentException("Too few parameters for metric query");
    // }
    // if (mParts.length > 5) {
    // throw new
    // IllegalArgumentException("Too many parameters for metric query");
    // }
    // // Aggregator is required, it's in the first section
    // sub.setAggregator(mParts[0]);
    // // Parse the rates from the 2nd through
    // // next to last sections
    // for (int i = 1; i < mParts.length - 1; i++) {
    // if (mParts[i].startsWith("rate")) {
    // sub.setRate(true);
    // final RateOption options = new RateOption();
    // if (mParts[i].equals("rate")) {
    // sub.setRateOptions(options);
    // } else {
    // final String rate = mParts[i].substring(5, mParts[i].length() - 1);
    // final String[] rateOptions = rate.split(",");
    // for (int x = 0; x < rateOptions.length; x++) {
    // switch (x) {
    // case 0:
    // options.setCounter(rateOptions[x].endsWith("counter"));
    // break;
    // case 1:
    // options.setCounterMax(Long.parseLong(rateOptions[x]));
    // break;
    // case 2:
    // options.setResetValue(Long.parseLong(rateOptions[x]));
    // break;
    // default:
    // }
    // }
    // sub.setRateOptions(options);
    // }
    // } else {
    // // downsample
    // sub.setDownsample(Optional.of(mParts[i]));
    // }
    // }
    // // Parse metric name and tags from last
    // // section
    // final String metricAndTags = mParts[mParts.length - 1];
    // final int idx = metricAndTags.indexOf('{');
    // if (-1 == idx) {
    // // metric only
    // sub.setMetric(metricAndTags);
    // } else {
    // sub.setMetric(metricAndTags.substring(0, idx));
    // // Only supporting one set of {} as we
    // // are not supporting filters right now.
    // if (!metricAndTags.endsWith("}")) {
    // throw new IllegalArgumentException("Tag section does not end with '}'");
    // }
    // final String[] tags = metricAndTags.substring(idx).split("}");
    // if (tags.length > 0) {
    // // Process the first set of tags, which
    // // are groupBy
    // final String groupingTags = tags[0];
    // final String[] gTags = groupingTags.substring(1,
    // groupingTags.length()).split(
    // ",");
    // for (final String tag : gTags) {
    // final String[] tParts = tag.split("=");
    // final Filter f = new Filter();
    // f.setGroupBy(true);
    // f.setTagk(tParts[0]);
    // f.setFilter(tParts[1]);
    // sub.addFilter(f);
    // }
    // if (tags.length > 1) {
    // // Process the first set of tags,
    // // which are groupBy
    // final String nonGroupingTags = tags[1];
    // final String[] ngTags = nonGroupingTags.substring(1,
    // nonGroupingTags.length()).split(",");
    // for (final String tag : ngTags) {
    // final String[] tParts = tag.split("=");
    // sub.addTag(tParts[0], tParts[1]);
    // }
    //
    // }
    // }
    //
    // }
    // query.addQuery(sub);
    // });
    // }
    // if (decoder.parameters().containsKey("tsuid")) {
    // decoder.parameters().get("tsuid").forEach(ts -> {
    // final SubQuery sub = new SubQuery();
    // final int colon = ts.indexOf(':');
    // if (-1 != colon) {
    // sub.setAggregator(ts.substring(0, colon));
    // }
    // for (final String tsuid : ts.substring(colon + 1).split(",")) {
    // sub.addTsuid(tsuid);
    // }
    // query.addQuery(sub);
    // });
    // }
    // requests.add(query);
    //
    // } else if (decoder.path().equals(API_SEARCH_LOOKUP)) {
    // final SearchLookupRequest search = new SearchLookupRequest();
    // if (!decoder.parameters().containsKey("m")) {
    // throw new IllegalArgumentException("m parameter is required for lookup");
    // }
    // final String m = decoder.parameters().get("m").get(0);
    // final int tagIdx = m.indexOf("{");
    // if (-1 == tagIdx) {
    // search.setQuery(m);
    // } else {
    // search.setQuery(m.substring(0, tagIdx));
    // final String[] tags = m.substring(tagIdx + 1, m.length() - 1).split(",");
    // for (final String tag : tags) {
    // final String[] tParts = tag.split("=");
    // final Tag t = new Tag(tParts[0], tParts[1]);
    // search.addTag(t);
    // }
    // }
    // if (decoder.parameters().containsKey("limit")) {
    // search.setLimit(Integer.parseInt(decoder.parameters().get("limit").get(0)));
    // }
    // requests.add(search);
    // } else if (decoder.path().equals(API_SUGGEST)) {
    // final SuggestRequest suggest = new SuggestRequest();
    // suggest.setType(decoder.parameters().get("type").get(0));
    // if (decoder.parameters().containsKey("q")) {
    // suggest.setQuery(Optional.of(decoder.parameters().get("q").get(0)));
    // }
    // if (decoder.parameters().containsKey("max")) {
    // suggest.setMax(Integer.parseInt(decoder.parameters().get("max").get(0)));
    // }
    // requests.add(suggest);
    // } else if (decoder.path().equals(API_METRICS)) {
    // requests.add(new MetricsRequest());
    // } else if (decoder.path().equals(API_LOGIN)) {
    // requests.add(new X509LoginRequest());
    // } else {
    // throw new UnsupportedOperationException();
    // }
    // LOG.trace("Parsed GET request {}", requests);
    // return requests;
    // }

}
