package com.github.vitalibo.spark.etl.facade;

import com.github.vitalibo.spark.emr.LivyClient;
import com.github.vitalibo.spark.emr.model.Batch;
import com.github.vitalibo.spark.emr.model.BatchState;
import com.github.vitalibo.spark.etl.Facade;
import com.github.vitalibo.spark.etl.Factory;
import com.github.vitalibo.spark.etl.StepFunctionProxy;
import com.github.vitalibo.spark.etl.model.ActivityException;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityOutput;
import com.github.vitalibo.spark.etl.model.transform.CreateBatchRequestTranslator;
import com.github.vitalibo.spark.etl.util.Rules;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.vitalibo.spark.emr.model.BatchState.*;

@RequiredArgsConstructor
public class SparkJobFacade implements Facade<SparkActivityInput, SparkActivityOutput> {

    private static final Logger logger = LoggerFactory.getLogger(SparkJobFacade.class);

    private final Rules<SparkActivityInput> rules;
    private final Factory.LivyClient livyFactory;

    private final int heartbeatPeriod;

    @Override
    @SneakyThrows(InterruptedException.class)
    public SparkActivityOutput process(final StepFunctionProxy proxy, SparkActivityInput input) {
        rules.verify(input);

        final LivyClient livy = livyFactory.create(input.getLivyHost(), input.getLivyPort());

        Batch batch = livy.createBatch(
                CreateBatchRequestTranslator.from(
                    input.getParameters()))
            .getBatch();
        logger.info("BatchId:{} - Started ...", batch.getId());

        BatchState state = batch.getState();
        while (state == RUNNING || state == STARTING) {

            logger.info("BatchId:{} - Sync heartbeat (State: {})", batch.getId(), state);
            if (!proxy.taskSyncHeartbeat()) {
                livy.killBatch(batch.getId());
                logger.info("BatchId:{} - Kill application " +
                    "(Reason: not sync heartbeat with AWS Step Function)", batch.getId());
                throw new ActivityException("Application was killed");
            }

            Thread.sleep(heartbeatPeriod);

            state = livy.getBatchState(batch.getId())
                .getState();
        }

        batch = livy.getBatch(batch.getId())
            .getBatch();

        if (state == DEAD) {
            logger.info("BatchId:{} - Failed ...", batch.getId());
            throw new ActivityException(
                String.format("Application %s failed (BatchId: %s)",
                    batch.getApplicationId(), batch.getId()));
        }

        logger.info("BatchId:{} - Successfully finished", batch.getId());
        return new SparkActivityOutput()
            .withApplicationId(batch.getApplicationId())
            .withBatchId(batch.getId());
    }

}