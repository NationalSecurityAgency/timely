package timely.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import timely.api.annotation.AnnotationResolver;
import timely.api.annotation.WebSocket;

public class JsonUtil {

    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new GuavaModule());
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
        // mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        SubtypeResolver resolver = mapper.getSubtypeResolver();
        AnnotationResolver.getWebSocketClasses().forEach(c -> {
            String name = c.getAnnotation(WebSocket.class).operation();
            log.trace("Registering subtype {} with class {}", name, c.getName());
            resolver.registerSubtypes(new NamedType(c, name));
        });
    }

    public static ObjectMapper getObjectMapper() {
        return mapper;
    }

}
