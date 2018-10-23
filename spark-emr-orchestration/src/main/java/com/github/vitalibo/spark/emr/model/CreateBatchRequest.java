package com.github.vitalibo.spark.emr.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@JsonInclude(Include.NON_NULL)
public class CreateBatchRequest {

    @JsonProperty(value = "file")
    private String file;

    @JsonProperty(value = "proxyUser")
    private String proxyUser;

    @JsonProperty(value = "className")
    private String className;

    @JsonProperty(value = "args")
    private List<String> args;

    @JsonProperty(value = "jars")
    private List<String> jars;

    @JsonProperty(value = "pyFiles")
    private List<String> pyFiles;

    @JsonProperty(value = "files")
    private List<String> files;

    @JsonProperty(value = "driverMemory")
    private String driverMemory;

    @JsonProperty(value = "driverCores")
    private Integer driverCores;

    @JsonProperty(value = "executorMemory")
    private String executorMemory;

    @JsonProperty(value = "executorCores")
    private Integer executorCores;

    @JsonProperty(value = "numExecutors")
    private Integer numExecutors;

    @JsonProperty(value = "archives")
    private List<String> archives;

    @JsonProperty(value = "queue")
    private String queue;

    @JsonProperty(value = "name")
    private String name;

    @JsonProperty(value = "conf")
    private Map<String, String> configuration;

    public CreateBatchRequest withFile(String file) {
        this.file = file;
        return this;
    }

    public CreateBatchRequest withProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
        return this;
    }

    public CreateBatchRequest withClassName(String className) {
        this.className = className;
        return this;
    }

    public CreateBatchRequest withArgs(List<String> args) {
        this.args = args;
        return this;
    }

    public CreateBatchRequest withJars(List<String> jars) {
        this.jars = jars;
        return this;
    }

    public CreateBatchRequest withPyFiles(List<String> pyFiles) {
        this.pyFiles = pyFiles;
        return this;
    }

    public CreateBatchRequest withFiles(List<String> files) {
        this.files = files;
        return this;
    }

    public CreateBatchRequest withDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
        return this;
    }

    public CreateBatchRequest withDriverCores(Integer driverCores) {
        this.driverCores = driverCores;
        return this;
    }

    public CreateBatchRequest withExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
        return this;
    }

    public CreateBatchRequest withExecutorCores(Integer executorCores) {
        this.executorCores = executorCores;
        return this;
    }

    public CreateBatchRequest withNumExecutors(Integer numExecutors) {
        this.numExecutors = numExecutors;
        return this;
    }

    public CreateBatchRequest withArchives(List<String> archives) {
        this.archives = archives;
        return this;
    }

    public CreateBatchRequest withQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public CreateBatchRequest withName(String name) {
        this.name = name;
        return this;
    }

    public CreateBatchRequest withConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
        return this;
    }

}