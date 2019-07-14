package com.github.vitalibo.spark.emr;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ValidationRules {

    public static final Requirements<String> requirementsLivyHost =
        new Requirements<>(
            "LivyHost",
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

    public static final Requirements<Integer> requirementsLivyPort =
        new Requirements<>(
            "LivyPort",
            ValidationRules.requireInRange(0, 65535));

    public static final Requirements<String> requirementsParameterFile =
        new Requirements<>(
            "Parameters.File",
            ValidationRules::requireNonNull,
            ValidationRules::requireNotEmpty);

    public static final Requirements<String> requirementsParameterClassName =
        new Requirements<>(
            "Parameters.ClassName",
            ValidationRules.requireMatchRegex(
                "([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*"));

    public static final Requirements<String> requirementsParameterDriverMemory =
        new Requirements<>(
            "Parameters.DriverMemory",
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

    public static final Requirements<Integer> requirementsParameterDriverCores =
        new Requirements<>(
            "Parameters.DriverCores",
            ValidationRules.requireGreaterThan(0));

    public static final Requirements<String> requirementsParameterExecutorMemory =
        new Requirements<>(
            "Parameters.ExecutorMemory",
            ValidationRules.requireMatchRegex("[0-9.]+[GgMm]"));

    public static final Requirements<Integer> requirementsParameterExecutorCores =
        new Requirements<>(
            "Parameters.ExecutorCores",
            ValidationRules.requireGreaterThan(0));

    public static final Requirements<Integer> requirementsParameterNumExecutors =
        new Requirements<>(
            "Parameters.NumExecutors",
            ValidationRules.requireGreaterThan(0));

    private ValidationRules() {
    }

    private static void requireNonNull(Object value, String fieldName) {
        if (Objects.isNull(value)) {
            throw new ValidationException(
                String.format("Required field '%s' can't be null.", fieldName));
        }
    }

    private static void requireNotEmpty(CharSequence value, String fieldName) {
        if (Objects.isNull(value) || value.length() == 0) {
            throw new ValidationException(
                String.format("Required field '%s' can't be empty.", fieldName));
        }
    }

    private static Requirement<Integer> requireInRange(Integer lowerBound, Integer upperBound) {
        return (value, fieldName) -> {
            if (Objects.isNull(value)) {
                return;
            }

            if (value < lowerBound || value > upperBound) {
                throw new ValidationException(
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
                throw new ValidationException(
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
                throw new ValidationException(
                    String.format("Required field '%s' don't matches regex `%s`.", fieldName, regex));
            }
        };
    }

    private interface Requirement<T> extends BiConsumer<T, String> {

        default void require(T value, String fieldName) {
            accept(value, fieldName);
        }
    }

    public static class Requirements<T> {

        private final String fieldName;
        private final Collection<Requirement<T>> requirements;

        @SafeVarargs
        Requirements(String fieldName, Requirement<T>... requirements) {
            this.fieldName = fieldName;
            this.requirements = Arrays.asList(requirements);
        }

        public void forEach(T value) {
            requirements.forEach(requirement -> requirement.require(value, fieldName));
        }
    }
}