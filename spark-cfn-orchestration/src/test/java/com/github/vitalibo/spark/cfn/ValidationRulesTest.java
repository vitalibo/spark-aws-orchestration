package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ValidationRulesTest {

    @DataProvider
    public Object[][] samplesLivyHost() {
        return new Object[][]{
            {"ip-10-1-234-56.us-west-1.compute.internal"}, {"10.1.234.56"}
        };
    }

    @Test(dataProvider = "samplesLivyHost")
    public void testVerifyLivyHost(String livyHost) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost(livyHost);

        ValidationRules.verifyLivyHost(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectLivyHost() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesIncorrectLivyHost",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$LivyHost.*")
    public void testFailVerifyLivyHost(String livyHost) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost(livyHost);

        ValidationRules.verifyLivyHost(properties);
    }

    @DataProvider
    public Object[][] samplesLivyPort() {
        return new Object[][]{
            {null}, {0}, {8998}, {65535}
        };
    }

    @Test(dataProvider = "samplesLivyPort")
    public void testVerifyLivyPort(Integer livyPort) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyPort(livyPort);

        ValidationRules.verifyLivyPort(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectLivyPort() {
        return new Object[][]{
            {-1}, {65536}
        };
    }

    @Test(dataProvider = "samplesIncorrectLivyPort",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$LivyPort.*")
    public void testFailVerifyLivyPort(Integer livyPort) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyPort(livyPort);

        ValidationRules.verifyLivyPort(properties);
    }

    @DataProvider
    public Object[][] samplesFile() {
        return new Object[][]{
            {"s3://spark/examples/jars/spark-examples-2.3.2.jar"}
        };
    }

    @Test(dataProvider = "samplesFile")
    public void testVerifyFile(String file) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setFile(file);

        ValidationRules.verifyFile(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectFile() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesIncorrectFile",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$File.*")
    public void testFailVerifyFile(String file) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setFile(file);

        ValidationRules.verifyFile(properties);
    }

    @DataProvider
    public Object[][] samplesClassName() {
        return new Object[][]{
            {null}, {"org.apache.spark.examples.SparkPi"}
        };
    }

    @Test(dataProvider = "samplesClassName")
    public void testVerifyClassName(String className) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setClassName(className);

        ValidationRules.verifyClassName(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectClassName() {
        return new Object[][]{
            {""}, {"1com.ClassName"}
        };
    }

    @Test(dataProvider = "samplesIncorrectClassName",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$ClassName.*")
    public void testFailVerifyClassName(String className) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setClassName(className);

        ValidationRules.verifyClassName(properties);
    }

    @DataProvider
    public Object[][] samplesDriverMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesDriverMemory")
    public void testVerifyDriverMemory(String driverMemory) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setDriverMemory(driverMemory);

        ValidationRules.verifyDriverMemory(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectDriverMemory() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesIncorrectDriverMemory",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$DriverMemory.*")
    public void testFailVerifyDriverMemory(String driverMemory) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setDriverMemory(driverMemory);

        ValidationRules.verifyDriverMemory(properties);
    }

    @DataProvider
    public Object[][] samplesExecutorMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesExecutorMemory")
    public void testVerifyExecutorMemory(String executorMemory) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setExecutorMemory(executorMemory);

        ValidationRules.verifyExecutorMemory(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectExecutorMemory() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesIncorrectExecutorMemory",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$ExecutorMemory.*")
    public void testFailVerifyExecutorMemory(String executorMemory) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setExecutorMemory(executorMemory);

        ValidationRules.verifyExecutorMemory(properties);
    }

    @DataProvider
    public Object[][] samplesDriverCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesDriverCores")
    public void testVerifyDriverCores(Integer driverCores) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setDriverCores(driverCores);

        ValidationRules.verifyDriverCores(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectDriverCores() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectDriverCores",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$DriverCores.*")
    public void testFailVerifyDriverCores(Integer driverCores) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setDriverCores(driverCores);

        ValidationRules.verifyDriverCores(properties);
    }

    @DataProvider
    public Object[][] samplesExecutorCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesExecutorCores")
    public void testVerifyExecutorCores(Integer executorCores) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setExecutorCores(executorCores);

        ValidationRules.verifyExecutorCores(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectExecutorCores() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectExecutorCores",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$ExecutorCores.*")
    public void testFailVerifyExecutorCores(Integer executorCores) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setExecutorCores(executorCores);

        ValidationRules.verifyExecutorCores(properties);
    }

    @DataProvider
    public Object[][] samplesNumExecutors() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesNumExecutors")
    public void testVerifyNumExecutors(Integer numExecutors) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setNumExecutors(numExecutors);

        ValidationRules.verifyNumExecutors(properties);
    }

    @DataProvider
    public Object[][] samplesIncorrectNumExecutors() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectNumExecutors",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkJob\\$NumExecutors.*")
    public void testFailVerifyNumExecutors(Integer numExecutors) {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setNumExecutors(numExecutors);

        ValidationRules.verifyNumExecutors(properties);
    }

}