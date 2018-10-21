package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ValidationRules {

    private static final String FIELD_SPARK_JOB_LIVY_HOST = "SparkJob$LivyHost";
    private static final String FIELD_SPARK_JOB_LIVY_PORT = "SparkJob$LivyPort";
    private static final String FIELD_SPARK_JOB_FILE = "SparkJob$File";
    private static final String FIELD_SPARK_JOB_CLASS_NAME = "SparkJob$ClassName";
    private static final String FIELD_SPARK_JOB_DRIVER_MEMORY = "SparkJob$DriverMemory";
    private static final String FIELD_SPARK_JOB_EXECUTOR_MEMORY = "SparkJob$ExecutorMemory";
    private static final String FIELD_SPARK_JOB_DRIVER_CORES = "SparkJob$DriverCores";
    private static final String FIELD_SPARK_JOB_EXECUTOR_CORES = "SparkJob$ExecutorCores";
    private static final String FIELD_SPARK_JOB_NUM_EXECUTORS = "SparkJob$NumExecutors";

    static void verifyLivyHost(SparkJobResourceProperties properties) {
        List<Requirement<String>> requirements = Arrays.asList(
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

        requirements.forEach(requirement ->
            requirement.require(properties.getLivyHost(), FIELD_SPARK_JOB_LIVY_HOST));
    }

    static void verifyLivyPort(SparkJobResourceProperties properties) {
        List<Requirement<Integer>> requirements = Arrays.asList(
            ValidationRules.requireInRange(0, 65535));

        requirements.forEach(requirement ->
            requirement.require(properties.getLivyPort(), FIELD_SPARK_JOB_LIVY_PORT));
    }

    static void verifyFile(SparkJobResourceProperties properties) {
        List<Requirement<String>> requirements = Arrays.asList(
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

        requirements.forEach(requirement ->
            requirement.require(properties.getFile(), FIELD_SPARK_JOB_FILE));
    }

    static void verifyClassName(SparkJobResourceProperties properties) {
        List<Requirement<String>> requirements = Arrays.asList(
            ValidationRules.requireMatchRegex("([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*"));

        requirements.forEach(requirement ->
            requirement.require(properties.getClassName(), FIELD_SPARK_JOB_CLASS_NAME));
    }

    static void verifyDriverMemory(SparkJobResourceProperties properties) {
        List<Requirement<String>> requirements = Arrays.asList(
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

        requirements.forEach(requirement ->
            requirement.require(properties.getDriverMemory(), FIELD_SPARK_JOB_DRIVER_MEMORY));
    }

    static void verifyDriverCores(SparkJobResourceProperties properties) {
        List<Requirement<Integer>> requirements = Arrays.asList(
            ValidationRules.requireGreaterThan(0));

        requirements.forEach(requirement ->
            requirement.require(properties.getDriverCores(), FIELD_SPARK_JOB_DRIVER_CORES));
    }

    static void verifyExecutorMemory(SparkJobResourceProperties properties) {
        List<Requirement<String>> requirements = Arrays.asList(
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

        requirements.forEach(requirement ->
            requirement.require(properties.getExecutorMemory(), FIELD_SPARK_JOB_EXECUTOR_MEMORY));
    }

    static void verifyExecutorCores(SparkJobResourceProperties properties) {
        List<Requirement<Integer>> requirements = Arrays.asList(
            ValidationRules.requireGreaterThan(0));

        requirements.forEach(requirement ->
            requirement.require(properties.getExecutorCores(), FIELD_SPARK_JOB_EXECUTOR_CORES));
    }

    static void verifyNumExecutors(SparkJobResourceProperties properties) {
        List<Requirement<Integer>> requirements = Arrays.asList(
            ValidationRules.requireGreaterThan(0));

        requirements.forEach(requirement ->
            requirement.require(properties.getNumExecutors(), FIELD_SPARK_JOB_NUM_EXECUTORS));
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (Objects.isNull(value)) {
            throw new ResourceProvisionException(String.format("Required field '%s' can't be null.", fieldName));
        }
    }

    private static void requireNotEmpty(CharSequence value, String fieldName) {
        if (Objects.isNull(value) || value.length() == 0) {
            throw new ResourceProvisionException(String.format("Required field '%s' can't be empty.", fieldName));
        }
    }

    private static Requirement<Integer> requireInRange(Integer lowerBound, Integer upperBound) {
        return (value, fieldName) -> {
            if (Objects.isNull(value)) {
                return;
            }

            if (value < lowerBound || value > upperBound) {
                throw new ResourceProvisionException(
                    String.format("Field '%s' value must be in range [%s, %s].", fieldName, lowerBound, upperBound));
            }
        };
    }

    private static Requirement<Integer> requireGreaterThan(Integer lowerBound) {
        return (value, fieldName) -> {
            if (Objects.isNull(value)) {
                return;
            }

            if (value < lowerBound) {
                throw new ResourceProvisionException(
                    String.format("Field '%s' value must be greater than %s.", fieldName, lowerBound));
            }
        };
    }

    private static Requirement<String> requireMatchRegex(String regex) {
        Pattern pattern = Pattern.compile(regex);
        return (value, fieldName) -> {
            if (Objects.isNull(value)) {
                return;
            }

            Matcher matcher = pattern.matcher(value);
            if (!matcher.matches()) {
                throw new ResourceProvisionException(String.format("Required field '%s' don't matches regex `%s`.", fieldName, regex));
            }
        };
    }

    interface Requirement<T> extends BiConsumer<T, String> {

        default void require(T value, String fieldName) {
            accept(value, fieldName);
        }
    }

}