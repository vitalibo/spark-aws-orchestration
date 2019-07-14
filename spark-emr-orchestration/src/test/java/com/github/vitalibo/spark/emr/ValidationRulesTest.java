package com.github.vitalibo.spark.emr;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.github.vitalibo.spark.emr.ValidationRules.*;

public class ValidationRulesTest {

    @DataProvider
    public Object[][] samplesLivyHost() {
        return new Object[][]{
            {"ip-10-1-234-56.us-west-1.compute.internal"}, {"10.1.234.56"}
        };
    }

    @Test(dataProvider = "samplesLivyHost")
    public void testRequirementLivyHostPass(String livyHost) {
        requirementsLivyHost.forEach(livyHost);
    }

    @DataProvider
    public Object[][] samplesLivyHostIncorrect() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesLivyHostIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*LivyHost.*")
    public void testFailRequirementLivyHost(String livyHost) {
        requirementsLivyHost.forEach(livyHost);
    }

    @DataProvider
    public Object[][] samplesLivyPort() {
        return new Object[][]{
            {null}, {0}, {8998}, {65535}
        };
    }

    @Test(dataProvider = "samplesLivyPort")
    public void testRequirementLivyPortPass(Integer livyPort) {
        requirementsLivyPort.forEach(livyPort);
    }

    @DataProvider
    public Object[][] samplesLivyPortIncorrect() {
        return new Object[][]{
            {-1}, {65536}
        };
    }

    @Test(dataProvider = "samplesLivyPortIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*LivyPort.*")
    public void testFailRequirementLivyPort(Integer livyPort) {
        requirementsLivyPort.forEach(livyPort);
    }

    @DataProvider
    public Object[][] samplesFile() {
        return new Object[][]{
            {"s3://spark/examples/jars/spark-examples-2.3.2.jar"}
        };
    }

    @Test(dataProvider = "samplesFile")
    public void testRequirementParameterFilePass(String file) {
        requirementsParameterFile.forEach(file);
    }

    @DataProvider
    public Object[][] samplesFileIncorrect() {
        return new Object[][]{
            {null}, {""}
        };
    }

    @Test(dataProvider = "samplesFileIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.File.*")
    public void testFailRequirementParameterFile(String file) {
        requirementsParameterFile.forEach(file);
    }

    @DataProvider
    public Object[][] samplesClassName() {
        return new Object[][]{
            {null}, {"org.apache.spark.examples.SparkPi"}
        };
    }

    @Test(dataProvider = "samplesClassName")
    public void testRequirementParameterClassNamePass(String className) {
        requirementsParameterClassName.forEach(className);
    }

    @DataProvider
    public Object[][] samplesClassNameIncorrect() {
        return new Object[][]{
            {""}, {"1com.ClassName"}
        };
    }

    @Test(dataProvider = "samplesClassNameIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ClassName.*")
    public void testRequirementParameterClassNameNotPass(String className) {
        requirementsParameterClassName.forEach(className);
    }

    @DataProvider
    public Object[][] samplesDriverMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesDriverMemory")
    public void testRequirementParameterDriverMemoryPass(String driverMemory) {
        requirementsParameterDriverMemory.forEach(driverMemory);
    }

    @DataProvider
    public Object[][] samplesDriverMemoryIncorrect() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesDriverMemoryIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverMemory.*")
    public void testFailRequirementParameterDriverMemory(String driverMemory) {
        requirementsParameterDriverMemory.forEach(driverMemory);
    }

    @DataProvider
    public Object[][] samplesExecutorMemory() {
        return new Object[][]{
            {null}, {"512m"}, {"512M"}, {"2g"}, {"2g"}
        };
    }

    @Test(dataProvider = "samplesExecutorMemory")
    public void testRequirementParameterExecutorMemoryPass(String executorMemory) {
        requirementsParameterExecutorMemory.forEach(executorMemory);
    }

    @DataProvider
    public Object[][] samplesExecutorMemoryIncorrect() {
        return new Object[][]{
            {""}, {"1Mb"}, {"1K"}, {"2Gb"}
        };
    }

    @Test(dataProvider = "samplesExecutorMemoryIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorMemory.*")
    public void testFailRequirementParameterExecutorMemory(String executorMemory) {
        requirementsParameterExecutorMemory.forEach(executorMemory);
    }

    @DataProvider
    public Object[][] samplesDriverCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesDriverCores")
    public void testRequirementParameterDriverCoresPass(Integer driverCores) {
        requirementsParameterDriverCores.forEach(driverCores);
    }

    @DataProvider
    public Object[][] samplesDriverCoresIncorrect() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesDriverCoresIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverCores.*")
    public void testFailRequirementParameterDriverCores(Integer driverCores) {
        requirementsParameterDriverCores.forEach(driverCores);
    }

    @DataProvider
    public Object[][] samplesExecutorCores() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesExecutorCores")
    public void testRequirementParameterExecutorCoresPass(Integer executorCores) {
        requirementsParameterExecutorCores.forEach(executorCores);
    }

    @DataProvider
    public Object[][] samplesExecutorCoresIncorrect() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesExecutorCoresIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorCores.*")
    public void testFailRequirementParameterExecutorCores(Integer executorCores) {
        requirementsParameterExecutorCores.forEach(executorCores);
    }

    @DataProvider
    public Object[][] samplesNumExecutors() {
        return new Object[][]{
            {null}, {1}, {256}
        };
    }

    @Test(dataProvider = "samplesNumExecutors")
    public void testRequirementParameterNumExecutorsPass(Integer numExecutors) {
        requirementsParameterNumExecutors.forEach(numExecutors);
    }

    @DataProvider
    public Object[][] samplesNumExecutorsIncorrect() {
        return new Object[][]{
            {-1}
        };
    }

    @Test(dataProvider = "samplesNumExecutorsIncorrect",
        expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.NumExecutors.*")
    public void testFailRequirementParameterNumExecutors(Integer numExecutors) {
        requirementsParameterNumExecutors.forEach(numExecutors);
    }

}