[![Build Status](https://travis-ci.org/felixgborrego/lib-spark-manager.svg?branch=master)](https://travis-ci.org/felixgborrego/lib-spark-manager)

## Description
Scala library to manage a Spark 1.x cluster deployed in EC2.
It allows you to launch your cluster on demand and submit jobs.

It makes direct use of aws-java-sdk to create the cluster,
and it allows you to deploy your jar to your ec2 cluster from a remote machine.

Spark sbt plugin: https://github.com/felixgborrego/lib-spark-manager

## Usage

The following code will create a new cluster and execute a Spark task 

```scala
    val conf = ConfigFactory.load("spark-cluster.conf")
    val clusterConfig = ClusterConf(conf)
    val cluster = manager.start(clusterConfig)

    // Submit a job
    manager.execute(jobConf)
```
