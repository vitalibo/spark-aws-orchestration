package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.etl.model.ActivityInput;
import com.github.vitalibo.spark.etl.model.ActivityOutput;

@FunctionalInterface
public interface Facade<Input extends ActivityInput, Output extends ActivityOutput> {

    Output process(ActivityWorker.Context context, Input input);

}