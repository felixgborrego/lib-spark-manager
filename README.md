![Build Status](https://travis-ci.org/felixgborrego/lib-spark-manager.svg?branch=master)

## Description
Scala library to manage a Spark cluster deployed in EC2.
It allows you to launch your cluster on demand and submit jobs.

It makes use of $SPARH_HOME/ec2/spark-ec script to create the cluster,
but unlike spark-submit, it allows you to deploy your jar to your ec2 cluster from a remote machine.

Spark sbt plugin: https://github.com/felixgborrego/lib-spark-manager

## Usage

The following code will create a new cluster and execute a Spark task 

```scala
// Configure client with our credentials and the path where is SPARK_HOME. 
  val conf = LocalConfig(
    localSparkHome = "/Users/.../spark-1.0.2-bin-hadoop1",
    keyPair = "",
    keyFile = "full path to file.pem",
    awsAccessKeyId = "AWS access key",
    awsSecretKey = "AWS secret key",
    region = "eu-west-1")

// Configure the requested cluster
val clusterConfig = ClusterConfig(MicroCluster)
 
val manager = SparkEc2Manager()

// Create a new cluster or get reference if it already exists.
val cluster = manager.createCluster(conf, clusterConfig) await

// submit task
val result = manager.executeJob("path to jar", "class name", cluster)
```

## Compile

It's a work in progress and it's not in any central repository yet so you'll need to compile and install this lib on your own before use it. 

sbt publish-local


## Roadmap

This project is still in an early stage, there are many things that should be improved:

- [X] Support 1.0.x
- [X] Allow auto stop the cluster after the job ends.
- [ ] Support 1.1.x
- [ ] ...

