package com.github.vitalibo.spark.cfn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.vitalibo.cfn.resource.model.ResourceProperties;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SparkJobResourceProperties extends ResourceProperties {

    @JsonProperty(value = "LivyHost")
    private String livyHost;

    @JsonProperty(value = "LivyPort")
    private Integer livyPort;

    @JsonProperty(value = "Parameters")
    private Parameters parameters;

    @Data
    public static class Parameters {

        @JsonProperty(value = "File")
        private String file;

        @JsonProperty(value = "ProxyUser")
        private String proxyUser;

        @JsonProperty(value = "ClassName")
        private String className;

        @JsonProperty(value = "Args")
        private List<String> args;

        @JsonProperty(value = "Jars")
        private List<String> jars;

        @JsonProperty(value = "PyFiles")
        private List<String> pyFiles;

        @JsonProperty(value = "Files")
        private List<String> files;

        @JsonProperty(value = "DriverMemory")
        private String driverMemory;

        @JsonProperty(value = "DriverCores")
        private Integer driverCores;

        @JsonProperty(value = "ExecutorMemory")
        private String executorMemory;

        @JsonProperty(value = "ExecutorCores")
        private Integer executorCores;

        @JsonProperty(value = "NumExecutors")
        private Integer numExecutors;

        @JsonProperty(value = "Archives")
        private List<String> archives;

        @JsonProperty(value = "Queue")
        private String queue;

        @JsonProperty(value = "Name")
        private String name;

        @JsonProperty(value = "Configuration")
        private Map<String, String> configuration;

    }

}