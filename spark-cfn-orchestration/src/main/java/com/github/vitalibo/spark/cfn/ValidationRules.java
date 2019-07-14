package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
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
        wrapValidationException(() ->
            requirementsParameterFile.forEach(
                properties.getFile()));
    }

    static void verifyParameterClassName(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterClassName.forEach(
                properties.getClassName()));
    }

    static void verifyParameterDriverMemory(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterDriverMemory.forEach(
                properties.getDriverMemory()));
    }

    static void verifyParameterDriverCores(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterDriverCores.forEach(
                properties.getDriverCores()));
    }

    static void verifyParameterExecutorMemory(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterExecutorMemory.forEach(
                properties.getExecutorMemory()));
    }

    static void verifyParameterExecutorCores(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterExecutorCores.forEach(
                properties.getExecutorCores()));
    }

    static void verifyParameterNumExecutors(SparkJobResourceProperties properties) {
        wrapValidationException(() ->
            requirementsParameterNumExecutors.forEach(
                properties.getNumExecutors()));
    }

    private static void wrapValidationException(Runnable f) {
        try {
            f.run();

        } catch (ValidationException e) {
            throw new ResourceProvisionException(e.getMessage());
        }
    }

}