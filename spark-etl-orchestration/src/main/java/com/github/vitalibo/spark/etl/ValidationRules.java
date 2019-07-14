package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Properties;

import static com.github.vitalibo.spark.emr.ValidationRules.*;

final class ValidationRules {

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

    static void verifyParameterFile(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterFile.forEach(
            properties.getFile());
    }

    static void verifyParameterClassName(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterClassName.forEach(
            properties.getClassName());
    }

    static void verifyParameterDriverMemory(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterDriverMemory.forEach(
            properties.getDriverMemory());
    }

    static void verifyParameterDriverCores(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterDriverCores.forEach(
            properties.getDriverCores());
    }

    static void verifyParameterExecutorMemory(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterExecutorMemory.forEach(
            properties.getExecutorMemory());
    }

    static void verifyParameterExecutorCores(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterExecutorCores.forEach(
            properties.getExecutorCores());
    }

    static void verifyParameterNumExecutors(SparkActivityInput activityInput) {
        Properties properties = activityInput.getProperties();

        requirementsParameterNumExecutors.forEach(
            properties.getNumExecutors());
    }

}