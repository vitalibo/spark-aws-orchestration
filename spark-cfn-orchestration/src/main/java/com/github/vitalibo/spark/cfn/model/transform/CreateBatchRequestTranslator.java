package com.github.vitalibo.spark.cfn.model.transform;

import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.model.CreateBatchRequest;

public final class CreateBatchRequestTranslator {

    private CreateBatchRequestTranslator() {
    }

    public static CreateBatchRequest from(SparkJobResourceProperties.Parameters parameters) {
        return new CreateBatchRequest()
            .withFile(parameters.getFile())
            .withProxyUser(parameters.getProxyUser())
            .withClassName(parameters.getClassName())
            .withArgs(parameters.getArgs())
            .withJars(parameters.getJars())
            .withPyFiles(parameters.getPyFiles())
            .withFiles(parameters.getFiles())
            .withDriverMemory(parameters.getDriverMemory())
            .withDriverCores(parameters.getDriverCores())
            .withExecutorMemory(parameters.getExecutorMemory())
            .withExecutorCores(parameters.getExecutorCores())
            .withNumExecutors(parameters.getNumExecutors())
            .withArchives(parameters.getArchives())
            .withQueue(parameters.getQueue())
            .withName(parameters.getName())
            .withConfiguration(parameters.getConfiguration());
    }

}