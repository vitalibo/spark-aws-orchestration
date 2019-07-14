package com.github.vitalibo.spark.etl;

import com.github.vitalibo.spark.emr.ValidationException;
import com.github.vitalibo.spark.etl.model.SparkActivityInput;
import com.github.vitalibo.spark.etl.model.SparkActivityInput.Parameters;
import org.testng.annotations.Test;

public class ValidationRulesTest {

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*LivyHost.*")
    public void testVerifyLivyHost() {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyHost("");

        ValidationRules.verifyLivyHost(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*LivyPort.*")
    public void testVerifyLivyPort() {
        SparkActivityInput activityInput = new SparkActivityInput();
        activityInput.setLivyPort(-1);

        ValidationRules.verifyLivyPort(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.File.*")
    public void testVerifyParameterFile() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new Parameters();
        activityInput.setParameters(parameters);
        parameters.setFile("");

        ValidationRules.verifyParameterFile(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ClassName.*")
    public void testVerifyParameterClassName() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new Parameters();
        activityInput.setParameters(parameters);
        parameters.setClassName("");

        ValidationRules.verifyParameterClassName(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverMemory.*")
    public void testVerifyParameterDriverMemory() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new SparkActivityInput.Parameters();
        activityInput.setParameters(parameters);
        parameters.setDriverMemory("");

        ValidationRules.verifyParameterDriverMemory(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorMemory.*")
    public void testVerifyParameterExecutorMemory() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new Parameters();
        activityInput.setParameters(parameters);
        parameters.setExecutorMemory("");

        ValidationRules.verifyParameterExecutorMemory(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.DriverCores.*")
    public void testVerifyParameterDriverCores() {
        SparkActivityInput activityInput = new SparkActivityInput();
        SparkActivityInput.Parameters parameters = new SparkActivityInput.Parameters();
        activityInput.setParameters(parameters);
        parameters.setDriverCores(-1);

        ValidationRules.verifyParameterDriverCores(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.ExecutorCores.*")
    public void testVerifyParameterExecutorCores() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new SparkActivityInput.Parameters();
        activityInput.setParameters(parameters);
        parameters.setExecutorCores(-1);

        ValidationRules.verifyParameterExecutorCores(activityInput);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Parameters\\.NumExecutors.*")
    public void testVerifyParameterNumExecutors() {
        SparkActivityInput activityInput = new SparkActivityInput();
        Parameters parameters = new Parameters();
        activityInput.setParameters(parameters);
        parameters.setNumExecutors(-1);

        ValidationRules.verifyParameterNumExecutors(activityInput);
    }

}