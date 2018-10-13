package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.AbstractFactory;
import com.github.vitalibo.cfn.resource.Facade;
import com.github.vitalibo.spark.cfn.facade.SparkJobCreateFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobDeleteFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobUpdateFacade;
import com.github.vitalibo.spark.cfn.model.ResourceType;
import lombok.Getter;

import java.util.Map;

public class Factory extends AbstractFactory<ResourceType> {

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    Factory(Map<String, String> env) {
        super(ResourceType.class);
    }

    @Override
    public Facade createCreateFacade(ResourceType resourceType) {
        return new SparkJobCreateFacade();
    }

    @Override
    public Facade createDeleteFacade(ResourceType resourceType) {
        return new SparkJobDeleteFacade();
    }

    @Override
    public Facade createUpdateFacade(ResourceType resourceType) {
        return new SparkJobUpdateFacade();
    }

}