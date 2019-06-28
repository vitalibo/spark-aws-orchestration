package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.etl.facade.SparkJobFacade;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class FactoryTest {

    private Factory factory;

    @BeforeMethod
    public void setUp() {
        factory = new Factory(Collections.singletonMap("AWS_REGION", "us-west-2"));
    }

    @Test
    public void testCreateDispatcher() {
        ActivityDispatcher actual = factory.createDispatcher("foo", "bar");

        Assert.assertNotNull(actual);
    }

    @Test
    public void testCreateWorker() {
        ActivityWorker actual = factory.createWorker();

        Assert.assertNotNull(actual);
    }

    @Test
    public void testCreateSparkJob() {
        Facade actual = factory.createSparkJob();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.getClass(), SparkJobFacade.class);
    }

}