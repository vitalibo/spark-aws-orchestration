package com.github.vitalibo.spark.etl.model.transform;

import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;

public final class CreateBatchRequestTranslator {

    private CreateBatchRequestTranslator() {
    }

    public static CreateBatchRequest from(SparkActivityInput.Parameters parameters) {
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