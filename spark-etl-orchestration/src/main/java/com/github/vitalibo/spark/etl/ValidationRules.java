package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Parameters;

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
        Parameters parameters = activityInput.getParameters();

        requirementsParameterFile.forEach(
            parameters.getFile());
    }

    static void verifyParameterClassName(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterClassName.forEach(
            parameters.getClassName());
    }

    static void verifyParameterDriverMemory(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterDriverMemory.forEach(
            parameters.getDriverMemory());
    }

    static void verifyParameterDriverCores(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterDriverCores.forEach(
            parameters.getDriverCores());
    }

    static void verifyParameterExecutorMemory(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterExecutorMemory.forEach(
            parameters.getExecutorMemory());
    }

    static void verifyParameterExecutorCores(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterExecutorCores.forEach(
            parameters.getExecutorCores());
    }

    static void verifyParameterNumExecutors(SparkActivityInput activityInput) {
        Parameters parameters = activityInput.getParameters();

        requirementsParameterNumExecutors.forEach(
            parameters.getNumExecutors());
    }

}