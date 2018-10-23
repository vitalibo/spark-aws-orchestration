package com.github.vitalibo.spark.emr;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LivyClientBuilderTest {

    @Mock
    private LivyClient mockLivyClient;
    @Captor
    private ArgumentCaptor<ClientConfig> captorClientConfig;

    private LivyClientBuilder spyLivyClientBuilder;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        spyLivyClientBuilder = Mockito.spy(LivyClientBuilder.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*host.*")
    public void testRequiredNonNullHost() {
        spyLivyClientBuilder
            .build();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = ".*port.*")
    public void testRequiredNonNullPort() {
        spyLivyClientBuilder
            .withHost("host")
            .build();
    }

    @Test
    public void testBuildDefaultProperties() {
        Mockito.doReturn(mockLivyClient)
            .when(spyLivyClientBuilder).build(Mockito.any(), Mockito.anyString());

        LivyClient actual = spyLivyClientBuilder
            .withHost("host")
            .withPort(1234)
            .build();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockLivyClient);
        Mockito.verify(spyLivyClientBuilder).build(captorClientConfig.capture(), Mockito.eq("host:1234"));
        ClientConfig config = captorClientConfig.getValue();
        Assert.assertEquals(config.getProperty(ClientProperties.CONNECT_TIMEOUT), 1000);
        Assert.assertEquals(config.getProperty(ClientProperties.READ_TIMEOUT), 500);
    }

    @Test
    public void testBuild() {
        Mockito.doReturn(mockLivyClient)
            .when(spyLivyClientBuilder).build(Mockito.any(), Mockito.anyString());

        LivyClient actual = spyLivyClientBuilder
            .withConnectionTimeout(12345)
            .withReadTimeout(234)
            .withHost("host")
            .withPort(1234)
            .build();

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, mockLivyClient);
        Mockito.verify(spyLivyClientBuilder).build(captorClientConfig.capture(), Mockito.eq("host:1234"));
        ClientConfig config = captorClientConfig.getValue();
        Assert.assertEquals(config.getProperty(ClientProperties.CONNECT_TIMEOUT), 12345);
        Assert.assertEquals(config.getProperty(ClientProperties.READ_TIMEOUT), 234);
    }

}