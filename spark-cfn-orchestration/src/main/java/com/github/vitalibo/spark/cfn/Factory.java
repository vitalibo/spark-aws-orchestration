package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.AbstractFactory;
import com.github.vitalibo.cfn.resource.Facade;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.facade.SparkJobCreateFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobDeleteFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobUpdateFacade;
import com.github.vitalibo.spark.cfn.model.CustomResourceType;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.emr.LivyClientBuilder;
import lombok.Getter;

import java.util.Map;

public class Factory extends AbstractFactory<CustomResourceType> {

    private static final Rules<SparkJobResourceProperties> SPARK_JOB_RULES =
        new Rules<>(
            ValidationRules::verifyLivyHost,
            ValidationRules::verifyLivyPort,
            ValidationRules::verifyFile,
            ValidationRules::verifyClassName,
            ValidationRules::verifyDriverMemory,
            ValidationRules::verifyDriverCores,
            ValidationRules::verifyExecutorMemory,
            ValidationRules::verifyExecutorCores,
            ValidationRules::verifyNumExecutors);

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    private final LivyClientSync.Factory livyFactory;

    Factory(Map<String, String> env) {
        super(CustomResourceType.class);
        this.livyFactory = (host, port) ->
            new LivyClientSync(
                new LivyClientBuilder()
                    .withHost(host)
                    .withPort(port)
                    .build());
    }

    @Override
    public Facade createCreateFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobCreateFacade(
                    SPARK_JOB_RULES,
                    livyFactory);
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public Facade createDeleteFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobDeleteFacade(
                    livyFactory);
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public Facade createUpdateFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobUpdateFacade(
                    SPARK_JOB_RULES,
                    livyFactory);
            default:
                throw new IllegalStateException();
        }
    }

}