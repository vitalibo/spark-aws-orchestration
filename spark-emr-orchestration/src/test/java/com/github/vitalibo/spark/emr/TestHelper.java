package com.github.vitalibo.spark.emr;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class TestHelper {

    public static String resourceAsString(String resource) {
        return new BufferedReader(new InputStreamReader(resourceAsInputStream(resource)))
            .lines().collect(Collectors.joining());
    }

    public static InputStream resourceAsInputStream(String resource) {
        return TestHelper.class.getResourceAsStream(resource);
    }

}
