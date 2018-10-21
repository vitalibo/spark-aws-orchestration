package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.emr.LivyClient;
import com.github.vitalibo.spark.emr.model.BatchState;
import com.github.vitalibo.spark.emr.model.CreateBatchRequest;
import com.github.vitalibo.spark.emr.model.GetBatchResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.github.vitalibo.spark.emr.model.BatchState.STARTING;

@RequiredArgsConstructor
public class LivyClientSync {

    @Delegate
    private final LivyClient livy;
    private final Supplier<Long> now;

    LivyClientSync(LivyClient livy) {
        this(livy, System::currentTimeMillis);
    }

    public GetBatchResponse createBatchSync(CreateBatchRequest request, long timeoutInMillis) {
        Integer batchId = livy.createBatch(request)
            .getBatch()
            .getId();

        BatchState state = waitRunning(
            batchId, now.get() + timeoutInMillis);

        switch (state) {
            case STARTING:
                livy.killBatch(batchId);
                throw new ResourceProvisionException("Spark application didn't start within 5 minutes");
            case DEAD:
                throw new ResourceProvisionException("Spark application starting failed");
        }

        return livy.getBatch(batchId);
    }

    @SneakyThrows
    BatchState waitRunning(Integer batchId, long deadline) {
        Supplier<BatchState> state = () ->
            livy.getBatchState(batchId)
                .getState();

        Supplier<Long> remainingTime = () ->
            deadline - now.get();

        while (state.get() == STARTING && remainingTime.get() > 5_000) {
            Thread.sleep(1000);
        }

        return state.get();
    }

    public interface Factory extends BiFunction<String, Integer, LivyClientSync> {

        default LivyClientSync create(String host, Integer port) {
            return apply(host, Objects.nonNull(port) ? port : 8998);
        }

    }

}