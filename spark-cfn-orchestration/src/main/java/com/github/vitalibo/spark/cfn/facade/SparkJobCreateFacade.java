package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.CreateFacade;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.ValidationRules;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import lombok.experimental.Delegate;

public class SparkJobCreateFacade implements CreateFacade<SparkJobResourceProperties, SparkJobResourceData> {

    @Delegate
    private final Rules<SparkJobResourceProperties> rules = new Rules<>(
        ValidationRules::verifyFile);

    @Override
    public SparkJobResourceData create(SparkJobResourceProperties properties, Context context) throws ResourceProvisionException {
        return new SparkJobResourceData()
            .withPhysicalResourceId(null)
            .withApplicationId(null)
            .withSessionId(null);
    }

}