package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.DeleteFacade;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;

public class SparkJobDeleteFacade implements DeleteFacade<SparkJobResourceProperties, SparkJobResourceData> {

    @Override
    public SparkJobResourceData delete(SparkJobResourceProperties properties, String physicalResourceId,
                                       Context context) throws ResourceProvisionException {
        return new SparkJobResourceData()
            .withPhysicalResourceId(physicalResourceId);
    }

}