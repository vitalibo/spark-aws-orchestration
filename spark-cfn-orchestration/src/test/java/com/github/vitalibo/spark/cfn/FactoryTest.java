package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.Facade;
import com.github.vitalibo.spark.cfn.model.CustomResourceType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

public class FactoryTest {

    private Factory factory;

    @BeforeMethod
    public void setUp() {
        factory = new Factory(Collections.emptyMap());
    }

    @Test
    public void testGetInstance() {
        Factory actual = Factory.getInstance();

        Assert.assertNotNull(actual);
    }

    @Test
    public void testCreateCreateSparkJobFacade() {
        Facade actual = factory.createCreateFacade(CustomResourceType.SparkJob);

        Assert.assertNotNull(actual);
    }

    @Test
    public void testCreateDeleteSparkJobFacade() {
        Facade actual = factory.createDeleteFacade(CustomResourceType.SparkJob);

        Assert.assertNotNull(actual);
    }

    @Test
    public void testCreateUpdateSparkJobFacade() {
        Facade actual = factory.createUpdateFacade(CustomResourceType.SparkJob);

        Assert.assertNotNull(actual);
    }

}