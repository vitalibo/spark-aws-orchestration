package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.AbstractFactory;
import com.github.vitalibo.cfn.resource.Facade;
import com.github.vitalibo.spark.cfn.facade.SparkJobCreateFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobDeleteFacade;
import com.github.vitalibo.spark.cfn.facade.SparkJobUpdateFacade;
import com.github.vitalibo.spark.cfn.model.CustomResourceType;
import lombok.Getter;

import java.util.Map;

public class Factory extends AbstractFactory<CustomResourceType> {

    @Getter(lazy = true)
    private static final Factory instance = new Factory(System.getenv());

    Factory(Map<String, String> env) {
        super(CustomResourceType.class);
    }

    @Override
    public Facade createCreateFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobCreateFacade();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public Facade createDeleteFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobDeleteFacade();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public Facade createUpdateFacade(CustomResourceType resourceType) {
        switch (resourceType) {
            case SparkJob:
                return new SparkJobUpdateFacade();
            default:
                throw new IllegalStateException();
        }
    }

}