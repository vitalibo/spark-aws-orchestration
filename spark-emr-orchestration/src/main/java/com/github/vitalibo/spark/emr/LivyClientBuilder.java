package com.github.vitalibo.spark.emr;

public class LivyClientBuilder {

    private String host;
    private Integer port;

    public LivyClientBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public LivyClientBuilder withPort(Integer port) {
        this.port = port;
        return this;
    }

    public LivyClient build() {
        return null;
    }

}
