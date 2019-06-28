package com.github.vitalibo.spark.etl.model;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ActivityTest {

    @Test
    public void testName() {
        Activity activity = new Activity()
            .withArn("arn:aws:states:us-west-2:1234567890:activity:spark-job");

        String actual = activity.getName();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, "spark-job");
    }

}