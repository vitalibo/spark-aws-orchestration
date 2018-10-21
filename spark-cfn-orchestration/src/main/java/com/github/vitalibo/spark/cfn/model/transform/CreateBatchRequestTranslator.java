package com.github.vitalibo.spark.cfn.model.transform;

import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.model.CreateBatchRequest;

public final class CreateBatchRequestTranslator {

    private CreateBatchRequestTranslator() {
    }

    public static CreateBatchRequest from(SparkJobResourceProperties properties) {
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