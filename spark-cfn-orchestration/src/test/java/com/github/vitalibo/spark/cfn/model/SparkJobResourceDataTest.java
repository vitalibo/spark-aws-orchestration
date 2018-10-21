package com.github.vitalibo.spark.cfn.model;

import com.github.vitalibo.spark.cfn.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SparkJobResourceDataTest {

    @Test
    public void testAsJson() {
        SparkJobResourceData resourceData = makeSparkJobResourceData();

        String actual = TestHelper.asJson(resourceData);

        Assert.assertEquals(
            actual, TestHelper.resourceAsJson("/SparkJobResourceData.json"));
    }

    @Test
    public void testFromJson() {
        SparkJobResourceData actual = TestHelper
            .resourceAsObject("/SparkJobResourceData.json", SparkJobResourceData.class);

        Assert.assertEquals(actual, makeSparkJobResourceData());
    }

    private static SparkJobResourceData makeSparkJobResourceData() {
        return new SparkJobResourceData()
            .withPhysicalResourceId("application_1234567890_0321#123")
            .withApplicationId("application_1234567890_0321")
            .withBatchId(123);
    }

}