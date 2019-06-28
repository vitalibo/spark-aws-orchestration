package com.github.vitalibo.spark.etl.model;

import com.github.vitalibo.spark.etl.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkActivityOutputTest {

    @Test
    public void testAsJson() {
        SparkActivityOutput activityOutput = makeSparkActivityOutput();

        String actual = TestHelper.asJson(activityOutput);

        Assert.assertEquals(
            actual, TestHelper.resourceAsJson("/SparkActivityOutput.json"));
    }

    @Test
    public void testFromJson() {
        SparkActivityOutput actual = TestHelper
            .resourceAsObject("/SparkActivityOutput.json", SparkActivityOutput.class);

        Assert.assertEquals(actual, makeSparkActivityOutput());
    }

    private static SparkActivityOutput makeSparkActivityOutput() {
        return new SparkActivityOutput()
            .withApplicationId("application_1234567890_0321")
            .withBatchId(123);
    }

}