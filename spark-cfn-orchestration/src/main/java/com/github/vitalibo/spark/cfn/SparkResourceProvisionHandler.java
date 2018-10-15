package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionHandler;
import com.github.vitalibo.spark.cfn.model.CustomResourceType;

public class SparkResourceProvisionHandler extends ResourceProvisionHandler<CustomResourceType> {

    public SparkResourceProvisionHandler() {
        super(Factory.getInstance());
    }

}