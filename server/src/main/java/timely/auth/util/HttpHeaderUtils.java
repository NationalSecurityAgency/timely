package timely.auth.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.netty.handler.codec.http.HttpHeaders;

import java.util.Collection;
import java.util.Map;

public class HttpHeaderUtils {

    public static Multimap<String, String> toMultimap(HttpHeaders headers) {
        Multimap<String, String> headerMultimap = HashMultimap.create();
        if (headers != null) {
            for (Map.Entry<String, String> e : headers.entries()) {
                headerMultimap.put(e.getKey(), e.getValue());
            }
        }
        return headerMultimap;
    }

    public static String getSingleHeader(Multimap<String, String> headers, String headerName, boolean enforceOneValue)
    throws IllegalArgumentException {
        String value = null;
        Collection<String> values = (headers == null) ? null : headers.get(headerName);
        if (values != null && !values.isEmpty()) {
            if (values.size() > 1 && enforceOneValue) {
                throw new IllegalArgumentException(headerName + " was specified multiple times, which is not allowed");
            }
            value = values.stream().findFirst().orElse(null);
        }
        return value;
    }
}
