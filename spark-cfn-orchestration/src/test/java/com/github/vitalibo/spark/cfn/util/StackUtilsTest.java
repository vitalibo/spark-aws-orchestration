package com.github.vitalibo.spark.cfn.util;

import com.github.vitalibo.spark.emr.model.Batch;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StackUtilsTest {

    @Test
    public void testCreatePhysicalResourceId() {
        Batch batch = batch("application_1234567890_0321", 123);

        String actual = StackUtils.createPhysicalResourceId(batch);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, "application_1234567890_0321#123");
    }

    @Test
    public void testBatchFromPhysicalResourceId() {
        Batch actual = StackUtils.batchFromPhysicalResourceId("application_1234567890_0321#123");

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getApplicationId(), "application_1234567890_0321");
        Assert.assertEquals(actual.getId(), (Integer) 123);
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {"application_1234567890_0321$123"}, {"application_1234567890_0321"},
            {"1234567890_0321#123"}, {"application_"}, {"#123"}, {""},
        };
    }

    @Test(dataProvider = "samples", expectedExceptions = IllegalStateException.class)
    public void testBatchFromIncorrectPhysicalResourceId(String physicalResourceId) {
        StackUtils.batchFromPhysicalResourceId(physicalResourceId);
    }

    private static Batch batch(String appId, Integer batchId) {
        return new Batch()
            .withBatchId(batchId)
            .withApplicationId(appId);
    }

}