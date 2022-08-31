package timely.auth.util;

import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.netty.handler.codec.http.HttpHeaders;

public class HttpHeaderUtils {

    public static Multimap<String,String> toMultimap(HttpHeaders headers) {
        Multimap<String,String> headerMultimap = HashMultimap.create();
        if (headers != null) {
            for (Map.Entry<String,String> e : headers.entries()) {
                // lower case for HTTP/2 compatibility; even for HTTP/1, these were always
                // case-insensitive
                headerMultimap.put(e.getKey().toLowerCase(), e.getValue());
            }
        }
        return headerMultimap;
    }

    public static String getSingleHeader(Multimap<String,String> headers, String headerName, boolean enforceOneValue) throws IllegalArgumentException {
        if (headers == null) {
            return null;
        } else {
            Set<String> values = new HashSet<>();
            values.addAll(headers.get(headerName));
            values.addAll(headers.get(headerName.toLowerCase()));
            if (values.size() > 1 && enforceOneValue) {
                throw new IllegalArgumentException(headerName + " was specified multiple times, which is not allowed");
            }
            return values.stream().findFirst().orElse(null);
        }
    }
}
