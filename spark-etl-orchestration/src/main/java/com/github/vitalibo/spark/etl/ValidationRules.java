package com.github.vitalibo.spark.etl;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Properties;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ValidationRules {

    private static final Requirements<String> requirementsLivyHost =
        new Requirements<>(
            "SparkActivityInput.LivyHost",
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

    private static final Requirements<Integer> requirementsLivyPort =
        new Requirements<>(
            "SparkActivityInput.LivyPort",
            ValidationRules.requireInRange(0, 65535));

    private static final Requirements<String> requirementsPropertyFile =
        new Requirements<>(
            "SparkActivityInput.Properties.File",
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

    private static final Requirements<String> requirementsPropertyClassName =
        new Requirements<>(
            "SparkActivityInput.Properties.ClassName",
            ValidationRules.requireMatchRegex(
                "([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*"));

    private static final Requirements<String> requirementsPropertyDriverMemory =
        new Requirements<>(
            "SparkActivityInput.Properties.DriverMemory",
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

    private static final Requirements<Integer> requirementsPropertyDriverCores =
        new Requirements<>(
            "SparkActivityInput.Properties.DriverCores",
            ValidationRules.requireGreaterThan(0));

    private static final Requirements<String> requirementsPropertyExecutorMemory =
        new Requirements<>(
            "SparkActivityInput.Properties.ExecutorMemory",
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

    private static final Requirements<Integer> requirementsPropertyExecutorCores =
        new Requirements<>(
            "SparkActivityInput.Properties.ExecutorCores",
            ValidationRules.requireGreaterThan(0));

    private static final Requirements<Integer> requirementsPropertyNumExecutors =
        new Requirements<>(
            "SparkActivityInput.Properties.NumExecutors",
            ValidationRules.requireGreaterThan(0));

    private ValidationRules() {
    }

    static void verifyLivyHost(SparkActivityInput activityInput) {
        requirementsLivyHost.forEach(
            activityInput.getLivyHost());
    }

    static void verifyLivyPort(SparkActivityInput activityInput) {
        requirementsLivyPort.forEach(
            activityInput.getLivyPort());
    }

    static void verifyPropertyFile(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyFile.forEach(
            properties.getFile());
    }

    static void verifyPropertyClassName(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyClassName.forEach(
            properties.getClassName());
    }

    static void verifyPropertyDriverMemory(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyDriverMemory.forEach(
            properties.getDriverMemory());
    }

    static void verifyPropertyDriverCores(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyDriverCores.forEach(
            properties.getDriverCores());
    }

    static void verifyPropertyExecutorMemory(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyExecutorMemory.forEach(
            properties.getExecutorMemory());
    }

    static void verifyPropertyExecutorCores(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyExecutorCores.forEach(
            properties.getExecutorCores());
    }

    static void verifyPropertyNumExecutors(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsPropertyNumExecutors.forEach(
            properties.getNumExecutors());
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

    private static class Requirements<T> {

        private final String fieldName;
        private final Collection<Requirement<T>> requirements;

        @SafeVarargs
        Requirements(String fieldName, Requirement<T>... requirements) {
            this.fieldName = fieldName;
            this.requirements = Arrays.asList(requirements);
        }

        void forEach(T value) {
            requirements.forEach(requirement -> requirement.require(value, fieldName));
        }
    }
}