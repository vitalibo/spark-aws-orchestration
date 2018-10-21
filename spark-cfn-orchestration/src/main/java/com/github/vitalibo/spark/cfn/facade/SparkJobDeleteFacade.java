package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.DeleteFacade;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.cfn.util.StackUtils;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SparkJobDeleteFacade implements DeleteFacade<SparkJobResourceProperties, SparkJobResourceData> {

    private final LivyClientSync.Factory livyFactory;

    @Override
    public SparkJobResourceData delete(SparkJobResourceProperties properties, String physicalResourceId,
                                       Context context) throws ResourceProvisionException {
        LivyClientSync livy = livyFactory.create(
                properties.getLivyHost(), properties.getLivyPort());

        livy.killBatch(
            StackUtils.batchFromPhysicalResourceId(physicalResourceId)
                .getId());

        return new SparkJobResourceData()
            .withPhysicalResourceId(physicalResourceId);
    }

}