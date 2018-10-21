package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.CreateFacade;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.cfn.model.transform.CreateBatchRequestTranslator;
import com.github.vitalibo.spark.cfn.util.StackUtils;
import com.github.vitalibo.spark.emr.model.Batch;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

@RequiredArgsConstructor
public class SparkJobCreateFacade implements CreateFacade<SparkJobResourceProperties, SparkJobResourceData> {

    @Delegate
    private final Rules<SparkJobResourceProperties> rules;
    private final LivyClientSync.Factory livyFactory;

    @Override
    public SparkJobResourceData create(SparkJobResourceProperties properties, Context context) throws ResourceProvisionException {
        LivyClientSync livy = livyFactory.create(
                properties.getLivyHost(), properties.getLivyPort());

        Batch batch = livy.createBatchSync(
                CreateBatchRequestTranslator.from(properties),
                context.getRemainingTimeInMillis())
            .getBatch();

        return new SparkJobResourceData()
            .withPhysicalResourceId(StackUtils.createPhysicalResourceId(batch))
            .withApplicationId(batch.getApplicationId())
            .withBatchId(batch.getId());
    }

}