package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.UpdateFacade;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.ValidationRules;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import lombok.experimental.Delegate;

public class SparkJobUpdateFacade implements UpdateFacade<SparkJobResourceProperties, SparkJobResourceData> {

    @Delegate
    private final Rules<SparkJobResourceProperties> rules = new Rules<>(
        ValidationRules::verifyFile);

    @Override
    public SparkJobResourceData update(SparkJobResourceProperties properties, SparkJobResourceProperties oldProperties,
                                       String physicalResourceId, Context context) throws ResourceProvisionException {
        return new SparkJobResourceData()
            .withPhysicalResourceId(physicalResourceId)
            .withApplicationId(null)
            .withSessionId(null);
    }

}