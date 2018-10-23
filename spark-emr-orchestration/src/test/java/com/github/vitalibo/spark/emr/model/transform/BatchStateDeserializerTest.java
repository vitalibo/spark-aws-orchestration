package com.github.vitalibo.spark.emr.model.transform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.github.vitalibo.spark.emr.model.BatchState;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.github.vitalibo.spark.emr.model.BatchState.*;

public class BatchStateDeserializerTest {

    @Mock
    private JsonParser mockJsonParser;
    @Mock
    private DeserializationContext mockDeserializationContext;

    private BatchStateDeserializer deserializer;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        deserializer = new BatchStateDeserializer();
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            sample(null, null),
            sample("starting", STARTING),
            sample("running", RUNNING),
            sample("Recovering", RECOVERING),
            sample("dead", DEAD),
            sample("success", SUCCESS)
        };
    }

    @Test(dataProvider = "samples")
    public void testDeserialize(String input, BatchState expected) throws IOException {
        Mockito.when(mockJsonParser.getText()).thenReturn(input);

        BatchState actual = deserializer.deserialize(mockJsonParser, mockDeserializationContext);

        Assert.assertEquals(actual, expected);
    }

    private static Object[] sample(String input, BatchState expected) {
        return new Object[]{input, expected};
    }

}