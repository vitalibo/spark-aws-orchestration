# Orchestration Apache Spark on Amazon EMR

This solution help you automate the deployment of Apache Spark applications on Amazon EMR.
Orchestration Apache Spark implemented via integration with AWS CloudFormation and AWS Step Functions.
Using AWS CloudFormation custom resource you can easily deploy your Apache Spark application on Amazon EMR cluster.
It is useful for deploying your application on dev environment for testing purpose or organize CI/CD of Real-Time application.
Integration with AWS Step Functions allow serverless* orchestration Apache Spark application and organize flexible Apache Spark based ETL pipeline.

[![Build Status](https://travis-ci.org/vitalibo/spark-aws-orchestration.svg?branch=master)](https://travis-ci.org/vitalibo/spark-aws-orchestration)

### Usage

This guide provide samples of deployment and orchestration Apache Spark applications on Amazon EMR.

##### Deploy Apache Spark application via AWS CloudFormation stack

One of the problems that solved this solution is deployment Apache Spark application.
Integration with AWS CloudFormation implemented via [Custom Resource](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/template-custom-resources.html) (high level diagram you can see below).

![CloudFormation_Diagram](https://www.lucidchart.com/publicSegments/view/6fcb2e74-8628-426c-8454-9240b6d95201/image.png)

The solution include the following steps:

1. The developer define `SparkJob` custom resource in CloudFormation template, which includes Apache Livy host name and Apache Spark parameters (`ClassName`, `NumExecutors`, `ExecutorCores` etc.).
2. Whenever anything changes in resource, AWS CloudFormation sends a request (`Create`/`Update`/`Delete`) to Apache Spark Job submitter.
3. Apache Spark Job submitter is a AWS Lambda that launch in VPC and interactive with Apache Spark through Apache Livy that launched on Amazon EMR in same VPC.

Following sample demonstrate usage AWS CloudFormation custom resource.

```yaml
SparkPi:
  Type: 'Custom::SparkJob'
  Properties:
    ServiceToken: !Fn::ImportValue 'spark-job-submitter'
    LivyHost: !GetAtt
      - EmrSparkCluster
      - MasterPublicDNS
    LivyPort: 8998
    Parameters: 
      Name: 'Spark Pi'
      ClassName: 'org.apache.spark.examples.SparkPi'
      NumExecutors: 1
      DriverMemory: '512m'
      ExecutorMemory: '512m'
      ExecutorCores: 1
      Args:
       - 10
```

##### Orchestration Apache Spark based ETL pipeline via AWS Step Functions

Another popular problem is manage ETL task.
Amazon EMR provide out of the box installation of [Apache Oozie](https://oozie.apache.org), but no integration with others AWS services and difficult configure, manage Apache Spark applications might not be suitable for some users.
Integration with AWS Step Functions allow simplify deployment and manage ETL tasks with moder UI and integrations with other AWS services (high level solution diagram you can see below).

![StepFunctions_Diagram](https://www.lucidchart.com/publicSegments/view/3c9f9fea-8076-46e6-bcba-276f17902064/image.png)

The solution include the following steps:

1. The developer define `spark-job` activity task in your state machine template using Amazon State Language, which includes Apache Livy host name and Apache Spark parameters (`ClassName`, `NumExecutors`, `ExecutorCores` etc.).
2. Trigger the AWS Step Function state machine by other external events, like CloudWatch Event or S3 object creation.
3. AWS Step Function referenced on activity Arn create new activity task and pass Apache Spark parameters as input state.
4. On Amazon EMR master instance running activity worker that polls activity task and interactive with Apache Spark through Apache Livy based on input state.
Every N seconds application synchronize Apache Spark job state and send heartbeat to AWS Step Function.

Following sample demonstrate usage AWS Step Functions custom Apache Spark activity.

```json
{
  "Comment": "Spark ETL State Machine",
  "StartAt": "SparkPi",
  "TimeoutSeconds": 300,
  "States":
  {
    "SparkPi": {
      "Type": "Task",
      "Resource": "arn:aws:states:us-east-1:123456789012:activity:example.spark-job",
      "Parameters": {
        "Name": "Spark Pi",
        "ClassName": "org.apache.spark.examples.SparkPi",
        "NumExecutors": 1,
        "DriverMemory": "512m",
        "ExecutorMemory": "512m",
        "ExecutorCores": 1,
        "Args": [
          "10"
        ]
      },
      "End": true
    }  
  }
}
```

### Links

- [Orchestrate Apache Spark applications using AWS Step Functions and Apache Livy](https://aws.amazon.com/blogs/big-data/orchestrate-apache-spark-applications-using-aws-step-functions-and-apache-livy/)
- [Build a Concurrent Data Orchestration Pipeline Using Amazon EMR and Apache Livy](https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/)
