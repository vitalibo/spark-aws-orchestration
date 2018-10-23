package com.github.vitalibo.spark.emr;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.ClientBuilder;
import java.util.Objects;

public class LivyClientBuilder {

    private static final Integer CONNECT_TIMEOUT_DEFAULT = 1000;
    private static final Integer READ_TIMEOUT_DEFAULT = 500;

    private String host;
    private Integer port;
    private Integer connectionTimeout = CONNECT_TIMEOUT_DEFAULT;
    private Integer readTimeout = READ_TIMEOUT_DEFAULT;

    public LivyClientBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public LivyClientBuilder withPort(Integer port) {
        this.port = port;
        return this;
    }

    public LivyClientBuilder withConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public LivyClientBuilder withReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    public LivyClient build() {
        Objects.requireNonNull(host, "Apache Livy host must be not null");
        Objects.requireNonNull(port, "Apache Livy port must be not null");

        return build(
            new ClientConfig()
                .property(ClientProperties.CONNECT_TIMEOUT, connectionTimeout)
                .property(ClientProperties.READ_TIMEOUT, readTimeout),
            String.format("%s:%s", host, port));
    }

    LivyClient build(ClientConfig config, String host) {
        return new LivyHttpClient(
            ClientBuilder.newClient(config),
            host);
    }

}