package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;

import java.util.Objects;

public final class ValidationRules {

    public static void verifyFile(SparkJobResourceProperties properties) {
        final String file = properties.getFile();

        requireNonNull(file, "File");
    }

    private static void requireNonNull(String value, String fieldName) {
        if (Objects.isNull(value) || value.isEmpty()) {
            throw new ResourceProvisionException(String.format("Required field '%s' can't by empty.", fieldName));
        }
    }

}