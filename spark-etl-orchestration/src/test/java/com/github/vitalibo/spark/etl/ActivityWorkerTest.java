package com.github.vitalibo.spark.etl;

import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.github.vitalibo.spark.etl.model.Activity;
import com.github.vitalibo.spark.etl.model.ActivityException;
import com.github.vitalibo.spark.etl.model.ActivityInput;
import com.github.vitalibo.spark.etl.model.ActivityOutput;
import lombok.Data;
import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;

public class ActivityWorkerTest {

    @Mock
    private AWSStepFunctions mockAWSStepFunctions;
    @Mock
    private ExecutorService mockExecutorService;
    @Mock
    private Facade mockFacade;
    @Mock
    private Activity mockActivity;
    @Captor
    private ArgumentCaptor<Runnable> captorFunction;
    @Mock
    private StepFunctionProxy mockStepFunctionProxy;

    private ActivityWorker spyWorker;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spyWorker = Mockito.spy(new ActivityWorker(mockAWSStepFunctions, mockExecutorService));
    }

    @Test
    public void testSubmit() {
        Mockito.doNothing()
            .when(spyWorker).submit(Mockito.any(), Mockito.eq(mockFacade), Mockito.eq(mockActivity));

        spyWorker.submit(mockFacade, mockActivity);

        Mockito.verify(mockExecutorService).submit(captorFunction.capture());
        captorFunction.getValue().run();
        Mockito.verify(spyWorker).submit(Mockito.any(), Mockito.eq(mockFacade), Mockito.eq(mockActivity));
    }

    @Test
    public void testSubmitTaskSuccess() {
        Activity activity = new Activity()
            .withJsonInput("{\"value\":\"foo\"}");
        class TestFacade implements Facade<TestActivityInput, TestActivityOutput> {
            @Override
            public TestActivityOutput process(StepFunctionProxy proxy, TestActivityInput input) {
                TestActivityOutput output = new TestActivityOutput();
                output.setValue("bar");
                return output;
            }
        }

        spyWorker.submit(mockStepFunctionProxy, new TestFacade(), activity);

        Mockito.verify(mockStepFunctionProxy).taskSuccess("{\"value\":\"bar\"}");
    }

    @Test
    public void testSubmitTaskFailure() {
        Activity activity = new Activity()
            .withJsonInput("{\"value\":\"foo\"}");
        class TestFacade implements Facade<TestActivityInput, TestActivityOutput> {
            @Override
            public TestActivityOutput process(StepFunctionProxy proxy, TestActivityInput input) {
                throw new ActivityException("bar");
            }
        }

        spyWorker.submit(mockStepFunctionProxy, new TestFacade(), activity);

        Mockito.verify(mockStepFunctionProxy)
            .taskFailure("com.github.vitalibo.spark.etl.model.ActivityException", "bar");
    }

    @Test
    public void testSubmitFacadeFailure() {
        Activity activity = new Activity()
            .withJsonInput("{\"value\":\"foo\"}");
        class TestFacade implements Facade<TestActivityInput, TestActivityOutput> {
            @Override
            public TestActivityOutput process(StepFunctionProxy proxy, TestActivityInput input) {
                throw new RuntimeException("bar");
            }
        }

        spyWorker.submit(mockStepFunctionProxy, new TestFacade(), activity);

        Mockito.verify(mockStepFunctionProxy)
            .taskFailure("java.lang.RuntimeException", "bar");
    }

    @Test
    public void testClose() {
        spyWorker.close();

        Mockito.verify(mockExecutorService).shutdown();
    }

    @Data
    private static class TestActivityInput extends ActivityInput {
        private String value;
    }

    @Data
    private static class TestActivityOutput extends ActivityOutput {
        private String value;
    }

}