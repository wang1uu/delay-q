package cc.wang1.component.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ResolvableType;
import org.springframework.util.StringUtils;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

public class Jsons {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    static {
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        javaTimeModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        javaTimeModule.addSerializer(LocalTime.class, new LocalTimeSerializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        javaTimeModule.addDeserializer(LocalTime.class, new LocalTimeDeserializer(DateTimeFormatter.ofPattern(DEFAULT_DATETIME_PATTERN)));
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                .registerModule(new ParameterNamesModule())
                .registerModule(javaTimeModule);
    }

    private static final Logger LOG = LoggerFactory.getLogger(Jsons.class);
    private final static PrettyPrinter DEFAULT_PRETTY_PRINTER = new DefaultPrettyPrinter();

    public static JsonNode readTree(ObjectMapper objectMapper, String content) {
        try {
            return objectMapper.readTree(content);
        } catch (Exception e) {
            LOG.error("Jsons.readTree error: ", e);
        }
        return null;
    }

    public static JsonNode readTree(String content) {
        return readTree(OBJECT_MAPPER, content);
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    public static <T> T convertValue(JsonNode node, TypeReference<T> toValueTypeRef) {
        try {
            return OBJECT_MAPPER.convertValue(node, toValueTypeRef);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }

    public static <T> T convertValue(JsonNode node, Class<T> toValueType) {
        try {
            return OBJECT_MAPPER.convertValue(node, toValueType);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }

    public static String toJson(Object obj) {
        return toJson(OBJECT_MAPPER, obj);
    }

    public static String toJson(ObjectMapper objectMapper, Object obj) {
        if (isNull(obj)) {
            return null;
        }
        if (obj instanceof String) {
            return (String) obj;
        }
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            LOG.error("Jsons.toJson error: ", e);
        }
        return null;
    }

    public static String toJsonWithDefaultPrettyPrinter(Object obj) {
        return toJsonWithPrettyPrinter(obj, DEFAULT_PRETTY_PRINTER);
    }

    public static String toJsonWithStandardPrettyPrinter(Object obj) {
        return toJsonWithPrettyPrinter(obj, DEFAULT_PRETTY_PRINTER);
    }

    public static String toJsonWithPrettyPrinter(Object obj, PrettyPrinter pp) {
        if (isNull(obj)) {
            return null;
        }
        if (obj instanceof String) {
            return (String) obj;
        }
        try {
            return OBJECT_MAPPER.writer(pp).writeValueAsString(obj);
        } catch (Exception e) {
            LOG.error("Jsons.toJson error: ", e);
        }
        return null;
    }

    public static <T> T toBean(String str, Type type) {
        return toBean(str, TypeFactory.defaultInstance().constructType(type));
    }

    public static <T> T toBean(String str, JavaType javaType) {
        return toBean(OBJECT_MAPPER, str, javaType);
    }

    public static <T> T toBean(ObjectMapper objectMapper, String str, Type type) {
        return toBean(objectMapper, str, TypeFactory.defaultInstance().constructType(type));
    }

    public static <T> T toBean(ObjectMapper objectMapper, String str, JavaType javaType) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        try {
            return objectMapper.readValue(str, javaType);
        } catch (Exception e) {
            LOG.error("Jsons.toBean error: ", e);
        }
        return null;
    }

    public static <T> T toBean(String str, Class<T> cls) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(str, cls);
        } catch (Exception e) {
            LOG.error("Jsons.toBean error: ", e);
        }
        return null;
    }

    public static <K, V> Map<K, V> toMap(String str, Class<K> key, Class<V> value) {
        ResolvableType resultType = ResolvableType.forClassWithGenerics(Map.class, key, value);
        return toBean(str, resultType.getType());
    }

    public static <T> List<T> toList(String str, Class<T> tClass) {
        ResolvableType resultType = ResolvableType.forClassWithGenerics(List.class, tClass);
        return toBean(str, resultType.getType());
    }

    public static <T> T toBean(String str, TypeReference<T> typeReference) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(str, typeReference);
        } catch (Exception e) {
            LOG.error("Jsons.toBean error: ", e);
        }
        return null;
    }

    public static <T> T convertValue(JsonNode jsonNode, Type type) {
        return convertValue(jsonNode, TypeFactory.defaultInstance().constructType(type));
    }

    public static <T> T convertValue(ObjectMapper objectMapper, JsonNode jsonNode, Type type) {
        return convertValue(objectMapper, jsonNode, TypeFactory.defaultInstance().constructType(type));
    }

    public static <T> T convertValue(JsonNode jsonNode, JavaType javaType) {
        return convertValue(OBJECT_MAPPER, jsonNode, javaType);
    }

    public static <T> T convertValue(ObjectMapper objectMapper, JsonNode jsonNode, JavaType javaType) {
        try {
            return objectMapper.convertValue(jsonNode, javaType);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }

    public static <T> T convertValue(Object o, Class<T> toValueType) {
        try {
            return OBJECT_MAPPER.convertValue(o, toValueType);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }

    public static <T> T convertValue(Object fromValue, TypeReference<T> toValueTypeRef) {
        try {
            return OBJECT_MAPPER.convertValue(fromValue, toValueTypeRef);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }

    public static <T> T convertValue(Object fromValue, JavaType toValueType) {
        try {
            return OBJECT_MAPPER.convertValue(fromValue, toValueType);
        } catch (Exception e) {
            LOG.error("Jsons.convertValue error: ", e);
        }
        return null;
    }
}
