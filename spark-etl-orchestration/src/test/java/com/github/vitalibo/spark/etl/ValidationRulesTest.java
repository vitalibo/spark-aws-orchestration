package com.github.vitalibo.spark.etl;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Properties;
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
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyHost(livyHost);

        ValidationRules.verifyLivyHost(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectLivyHost() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesIncorrectLivyHost",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.LivyHost.*")
    public void testFailVerifyLivyHost(String livyHost) {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyHost(livyHost);

        ValidationRules.verifyLivyHost(activityInput);
    }

    @DataProvider
    public Object[][] samplesLivyPort() {
        return new Object[][]{
            {null}, {0}, {8998}, {65535}
        };
    }

    @Test(dataProvider = "samplesLivyPort")
    public void testVerifyLivyPort(Integer livyPort) {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyPort(livyPort);

        ValidationRules.verifyLivyPort(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectLivyPort() {
        return new Object[][]{
            {-1}, {65536}
        };
    }

    @Test(dataProvider = "samplesIncorrectLivyPort",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.LivyPort.*")
    public void testFailVerifyLivyPort(Integer livyPort) {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyPort(livyPort);

        ValidationRules.verifyLivyPort(activityInput);
    }

    @DataProvider
    public Object[][] samplesFile() {
        return new Object[][]{
            {"s3://spark/examples/jars/spark-examples-2.3.2.jar"}
        };
    }

    @Test(dataProvider = "samplesFile")
    public void testVerifyFile(String file) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setFile(file);

        ValidationRules.verifyPropertyFile(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectFile() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesIncorrectFile",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.File.*")
    public void testFailVerifyFile(String file) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setFile(file);

        ValidationRules.verifyPropertyFile(activityInput);
    }

    @DataProvider
    public Object[][] samplesClassName() {
        return new Object[][]{
            {null}, {"org.apache.spark.examples.SparkPi"}
        };
    }

    @Test(dataProvider = "samplesClassName")
    public void testVerifyClassName(String className) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setClassName(className);

        ValidationRules.verifyPropertyClassName(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectClassName() {
        return new Object[][]{
            {""}, {"1com.ClassName"}
        };
    }

    @Test(dataProvider = "samplesIncorrectClassName",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.ClassName.*")
    public void testFailVerifyClassName(String className) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setClassName(className);

        ValidationRules.verifyPropertyClassName(activityInput);
    }

    @DataProvider
    public Object[][] samplesDriverMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesDriverMemory")
    public void testVerifyDriverMemory(String driverMemory) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setDriverMemory(driverMemory);

        ValidationRules.verifyPropertyDriverMemory(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectDriverMemory() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesIncorrectDriverMemory",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.DriverMemory.*")
    public void testFailVerifyDriverMemory(String driverMemory) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setDriverMemory(driverMemory);

        ValidationRules.verifyPropertyDriverMemory(activityInput);
    }

    @DataProvider
    public Object[][] samplesExecutorMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesExecutorMemory")
    public void testVerifyExecutorMemory(String executorMemory) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setExecutorMemory(executorMemory);

        ValidationRules.verifyPropertyExecutorMemory(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectExecutorMemory() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesIncorrectExecutorMemory",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.ExecutorMemory.*")
    public void testFailVerifyExecutorMemory(String executorMemory) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setExecutorMemory(executorMemory);

        ValidationRules.verifyPropertyExecutorMemory(activityInput);
    }

    @DataProvider
    public Object[][] samplesDriverCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesDriverCores")
    public void testVerifyDriverCores(Integer driverCores) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setDriverCores(driverCores);

        ValidationRules.verifyPropertyDriverCores(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectDriverCores() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectDriverCores",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.DriverCores.*")
    public void testFailVerifyDriverCores(Integer driverCores) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setDriverCores(driverCores);

        ValidationRules.verifyPropertyDriverCores(activityInput);
    }

    @DataProvider
    public Object[][] samplesExecutorCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesExecutorCores")
    public void testVerifyExecutorCores(Integer executorCores) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setExecutorCores(executorCores);

        ValidationRules.verifyPropertyExecutorCores(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectExecutorCores() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectExecutorCores",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.ExecutorCores.*")
    public void testFailVerifyExecutorCores(Integer executorCores) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setExecutorCores(executorCores);

        ValidationRules.verifyPropertyExecutorCores(activityInput);
    }

    @DataProvider
    public Object[][] samplesNumExecutors() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesNumExecutors")
    public void testVerifyNumExecutors(Integer numExecutors) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setNumExecutors(numExecutors);

        ValidationRules.verifyPropertyNumExecutors(activityInput);
    }

    @DataProvider
    public Object[][] samplesIncorrectNumExecutors() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesIncorrectNumExecutors",
        expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*SparkActivityInput.Properties.NumExecutors.*")
    public void testFailVerifyNumExecutors(Integer numExecutors) {
        SparkActivityInput activityInput = new SparkActivityInput();
        Properties properties = new Properties();
        activityInput.setProperties(properties);
        properties.setNumExecutors(numExecutors);

        ValidationRules.verifyPropertyNumExecutors(activityInput);
    }

}