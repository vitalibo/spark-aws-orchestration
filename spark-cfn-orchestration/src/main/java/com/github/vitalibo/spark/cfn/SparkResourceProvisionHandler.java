package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionHandler;
import com.github.vitalibo.spark.cfn.model.ResourceType;

public class SparkResourceProvisionHandler extends ResourceProvisionHandler<ResourceType> {

    public SparkResourceProvisionHandler() {
        super(Factory.getInstance());
    }

}