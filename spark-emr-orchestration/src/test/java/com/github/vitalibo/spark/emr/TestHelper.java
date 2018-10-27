package com.github.vitalibo.spark.emr;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public final class TestHelper {

    private static final ObjectMapper jackson = new ObjectMapper();

    static {
        jackson.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        jackson.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    private TestHelper() {
    }

    public static String resourceAsString(String resource) {
        return new BufferedReader(new InputStreamReader(resourceAsInputStream(resource)))
            .lines().collect(Collectors.joining());
    }

    public static InputStream resourceAsInputStream(String resource) {
        return TestHelper.class.getResourceAsStream(resource);
    }

    @SneakyThrows
    public static String resourceAsJson(String resource) {
        return asJson(resourceAsObject(resource, Object.class));
    }

    @SneakyThrows
    public static <T> T resourceAsObject(String resource, Class<T> cls) {
        return jackson.readValue(resourceAsInputStream(resource), cls);
    }

    @SneakyThrows
    public static String asJson(Object obj) {
        return jackson.writerWithDefaultPrettyPrinter()
            .writeValueAsString(obj);
    }

}