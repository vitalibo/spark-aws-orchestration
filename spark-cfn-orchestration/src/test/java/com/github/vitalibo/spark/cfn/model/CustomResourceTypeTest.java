package com.github.vitalibo.spark.cfn.model;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CustomResourceTypeTest {

    @Test
    public void testSparkJob() {
        CustomResourceType actual = CustomResourceType.SparkJob;

        Assert.assertEquals(actual.getTypeName(), "Custom::SparkJob");
        Assert.assertEquals(actual.getTypeClass(), SparkJobResourceProperties.class);
    }

}