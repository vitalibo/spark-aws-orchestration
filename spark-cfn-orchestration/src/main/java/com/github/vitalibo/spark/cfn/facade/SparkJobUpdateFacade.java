package com.github.vitalibo.spark.cfn.facade;

import com.amazonaws.services.lambda.runtime.Context;
import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.cfn.resource.facade.UpdateFacade;
import com.github.vitalibo.cfn.resource.util.Rules;
import com.github.vitalibo.spark.cfn.LivyClientSync;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceData;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.cfn.model.transform.CreateBatchRequestTranslator;
import com.github.vitalibo.spark.cfn.util.StackUtils;
import com.github.vitalibo.spark.emr.model.Batch;
import com.github.vitalibo.spark.emr.model.BatchState;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;


@RequiredArgsConstructor
public class SparkJobUpdateFacade implements UpdateFacade<SparkJobResourceProperties, SparkJobResourceData> {

    @Delegate
    private final Rules<SparkJobResourceProperties> rules;
    private final LivyClientSync.Factory livyFactory;

    @Override
    public SparkJobResourceData update(SparkJobResourceProperties properties, SparkJobResourceProperties oldProperties,
                                       String physicalResourceId, Context context) throws ResourceProvisionException {
        LivyClientSync livy = livyFactory.create(
                properties.getLivyHost(), properties.getLivyPort());

        Batch batch = livy.createBatchSync(
                CreateBatchRequestTranslator.from(
                    properties.getParameters()),
                context.getRemainingTimeInMillis())
            .getBatch();

        if (BatchState.RUNNING == batch.getState()) {
            // Kill old application to avoid parallel running two applications
            deleteOld(oldProperties, physicalResourceId);
        }

        return new SparkJobResourceData()
            .withPhysicalResourceId(StackUtils.createPhysicalResourceId(batch))
            .withApplicationId(batch.getApplicationId())
            .withBatchId(batch.getId());
    }

    void deleteOld(SparkJobResourceProperties oldProperties, String physicalResourceId) {
        LivyClientSync livy = livyFactory.create(
                oldProperties.getLivyHost(), oldProperties.getLivyPort());

        livy.killBatch(
            StackUtils.batchFromPhysicalResourceId(physicalResourceId)
                .getId());
    }

}