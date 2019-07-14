package com.github.vitalibo.spark.cfn;

import com.github.vitalibo.cfn.resource.ResourceProvisionException;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties;
import com.github.vitalibo.spark.cfn.model.SparkJobResourceProperties.Parameters;
import org.testng.annotations.Test;

public class ValidationRulesTest {

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*LivyHost.*")
    public void testVerifyLivyHost() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyHost("");

        ValidationRules.verifyLivyHost(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*LivyPort.*")
    public void testVerifyLivyPort() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        properties.setLivyPort(-1);

        ValidationRules.verifyLivyPort(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.File.*")
    public void testVerifyParameterFile() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setFile("");

        ValidationRules.verifyParameterFile(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ClassName.*")
    public void testVerifyParameterClassName() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setClassName("");

        ValidationRules.verifyParameterClassName(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverMemory.*")
    public void testVerifyParameterDriverMemory() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setDriverMemory("");

        ValidationRules.verifyParameterDriverMemory(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorMemory.*")
    public void testVerifyParameterExecutorMemory() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setExecutorMemory("");

        ValidationRules.verifyParameterExecutorMemory(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverCores.*")
    public void testVerifyParameterDriverCores() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setDriverCores(-1);

        ValidationRules.verifyParameterDriverCores(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorCores.*")
    public void testVerifyParameterExecutorCores() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setExecutorCores(-1);

        ValidationRules.verifyParameterExecutorCores(properties);
    }

    @Test(expectedExceptions = ResourceProvisionException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.NumExecutors.*")
    public void testVerifyParameterNumExecutors() {
        SparkJobResourceProperties properties = new SparkJobResourceProperties();
        Parameters parameters = new Parameters();
        properties.setParameters(parameters);
        parameters.setNumExecutors(-1);

        ValidationRules.verifyParameterNumExecutors(properties);
    }

}