package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties.Parameters;
import com.github.vitalibo.spark.emr.ValidationException;

import static com.github.vitalibo.spark.emr.ValidationRules.*;

final class ValidationRules {

    private ValidationRules() {
    }

    static void verifyLivyHost(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsLivyHost.forEach(
                properties.getLivyHost()));
    }

    static void verifyLivyPort(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsLivyPort.forEach(
                properties.getLivyPort()));
    }

    static void verifyParameterFile(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterFile.forEach(
                parameters.getFile()));
    }

    static void verifyParameterClassName(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterClassName.forEach(
                parameters.getClassName()));
    }

    static void verifyParameterDriverMemory(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterDriverMemory.forEach(
                parameters.getDriverMemory()));
    }

    static void verifyParameterDriverCores(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterDriverCores.forEach(
                parameters.getDriverCores()));
    }

    static void verifyParameterExecutorMemory(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterExecutorMemory.forEach(
                parameters.getExecutorMemory()));
    }

    static void verifyParameterExecutorCores(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterExecutorCores.forEach(
                parameters.getExecutorCores()));
    }

    static void verifyParameterNumExecutors(SparkJobResourceProperties properties) {
        Parameters parameters = properties.getParameters();

        wrapValidationException(() ->
            requirementsParameterNumExecutors.forEach(
                parameters.getNumExecutors()));
    }

    private static void wrapValidationException(Runnable f) {
        try {
            f.run();

        } catch (ValidationException e) {
            throw new ResourceProvisionException(e.getMessage());
        }
    }

}