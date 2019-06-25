package com.github.vitalibo.spark.etl.model.transform;

import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;

public final class CreateBatchRequestTranslator {

    private CreateBatchRequestTranslator() {
    }

    public static CreateBatchRequest from(SparkActivityInput.Properties properties) {
        return new CreateBatchRequest()
            .withFile(properties.getFile())
            .withProxyUser(properties.getProxyUser())
            .withClassName(properties.getClassName())
            .withArgs(properties.getArgs())
            .withJars(properties.getJars())
            .withPyFiles(properties.getPyFiles())
            .withFiles(properties.getFiles())
            .withDriverMemory(properties.getDriverMemory())
            .withDriverCores(properties.getDriverCores())
            .withExecutorMemory(properties.getExecutorMemory())
            .withExecutorCores(properties.getExecutorCores())
            .withNumExecutors(properties.getNumExecutors())
            .withArchives(properties.getArchives())
            .withQueue(properties.getQueue())
            .withName(properties.getName())
            .withConfiguration(properties.getConfiguration());
    }

}