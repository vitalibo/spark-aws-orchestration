package com.github.vitalibo.spark.etl.facade;

import com.github.vitalibo.spark.emr.LivyClient;
import com.github.vitalibo.spark.emr.model.Batch;
import com.github.vitalibo.spark.emr.model.BatchState;
import com.github.vitalibo.spark.emr.model.GetBatchLogRequest;
import com.github.vitalibo.spark.etl.ActivityException;
import com.github.vitalibo.spark.etl.ActivityWorker;
import com.github.vitalibo.spark.etl.Facade;
import com.github.vitalibo.spark.etl.Factory;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityOutput;
import com.github.vitalibo.spark.etl.model.transform.CreateBatchRequestTranslator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.util.List;

import static com.github.vitalibo.spark.emr.model.BatchState.*;

@RequiredArgsConstructor
public class SparkJobFacade implements Facade<SparkActivityInput, SparkActivityOutput> {

    private final Factory.LivyClient livyFactory;
    private final int heartbeatInterval;
    private final int logSize;

    @Override
    @SneakyThrows(InterruptedException.class)
    public SparkActivityOutput process(ActivityWorker.Context context, SparkActivityInput input) {
        LivyClient livy = livyFactory.create(input.getLivyHost(), input.getLivyPort());

        Batch batch = livy
            .createBatch(
                CreateBatchRequestTranslator.from(input.getProperties()))
            .getBatch();

        BatchState state = batch.getState();
        while (state == RUNNING || state == STARTING) {
            context.heartbeat();
            Thread.sleep(heartbeatInterval);

            state = livy.getBatchState(batch.getId())
                .getState();
        }

        if (state == DEAD) {
            batch = livy.getBatch(batch.getId())
                .getBatch();

            List<String> log = livy
                .getBatchLog(new GetBatchLogRequest()
                    .withBatchId(batch.getId())
                    .withSize(logSize))
                .getLog();

            throw new ActivityException(
                String.format("Application %s failed (batchId: %s)", batch.getApplicationId(), batch.getId()),
                String.join("\n", log));
        }

        return new SparkActivityOutput()
            .withApplicationId(batch.getApplicationId())
            .withBatchId(batch.getId());
    }

}