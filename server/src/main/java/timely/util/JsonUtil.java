package timely.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.annotation.AnnotationResolver;
import timely.api.annotation.WebSocket;

public class JsonUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class);
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new Jdk8Module());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        // mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        // Register the subtypes dynamically based on the WebSocket annotation
        // instead of having
        // to register them statically on this class using:
        // @JsonSubTypes({ @Type(name = "create", value =
        // CreateSubscription.class),
        // @Type(name = "add", value = AddSubscription.class),
        // @Type(name = "remove", value = RemoveSubscription.class),
        // @Type(name = "close", value = CloseSubscription.class) })
        SubtypeResolver resolver = mapper.getSubtypeResolver();
        AnnotationResolver.getWebSocketClasses().forEach(c -> {
            String name = c.getAnnotation(WebSocket.class).operation();
            LOG.trace("Registering subtype {} with class {}", name, c.getName());
            resolver.registerSubtypes(new NamedType(c, name));
        });
    }

    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

}
