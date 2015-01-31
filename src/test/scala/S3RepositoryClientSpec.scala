package com.fgb.spark.impl
import java.io.{InputStream, File}

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.{ AmazonS3URI }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class S3RepositoryClientSpec extends TestUtil {

  "S3RepositoryClient" should {

    "return the assigned s3 url for a jar released" in {
      val (amazonS3, s3client) = getS3Mocks
      val jarUrl = s3client.jarS3Url("jobName", "version", clusterConfig).toString
      val scriptUrl = s3client.scriptS3Url("jobName", "version", clusterConfig).toString
      jarUrl must beEqualTo("s3://bucket/jobs/jobName/jobName-spark-1.1.0-bin-hadoop1-version.jar")
      scriptUrl must beEqualTo("s3://bucket/jobs/jobName/jobName-spark-1.1.0-bin-hadoop1-version.sh")
      there was noCallsTo(amazonS3)
    }

    "return an exception when the release can't be done" in {
      val (amazonS3, s3client) = getS3Mocks
      val path = s3client.releaseJar(new File("test.jar"), "jobName", "version", clusterConfig.copy(bucket = "invalidBucket"))
      Await.result(path, Duration.Inf) must throwA[AmazonClientException]
      there was one(amazonS3).putObject(anyString, anyString, any[File])
    }

    "deployed the jar" in {
      val (amazonS3, s3client) = getS3Mocks
      val path = s3client.releaseJar(new File("test.jar"), "jobName", "version", clusterConfig).map(_.toString)

      Await.result(path, Duration.Inf) must beEqualTo("s3://bucket/jobs/jobName/jobName-spark-1.1.0-bin-hadoop1-version.jar")
      there was one(amazonS3).putObject(anyString, anyString, any[File])
    }

    "build a valid launch script" in {
      val (amazonS3, s3client) = getS3Mocks
      val s3Url = new AmazonS3URI("s3://myBucket/my/url/job.jar")

      val script = s3client.buildScript("com.test.spark.jobs.Test1", s3Url, clusterConfig)

      script must beEqualTo(
        """#!/bin/bash
          |#Script to submit a Spark job
          |echo Coping s3://myBucket/my/url/job.jar to /tmp/my/url/job.jar
          |mkdir -p /tmp/my/url
          |hadoop fs -get s3://myBucket/my/url/job.jar /tmp/my/url/job.jar
          |echo Submitting job to spark cluster spark-submit --class com.test.spark.jobs.Test1 /tmp/my/url/job.jar
          |/home/hadoop/spark/bin/spark-submit --class com.test.spark.jobs.Test1 /tmp/my/url/job.jar
          |echo Job submitted!""".stripMargin)
      there was noCallsTo(amazonS3)
    }

    "deploy a bootStrap script for Spark" in {
      val (amazonS3, s3client) = getS3Mocks
      val url = Await.result(s3client.releaseBootstraps(clusterConfig), Duration.Inf)
      url.toString must beEqualTo("s3://bucket/jobs/test/bootstraps/install-spark-1.1.0-bin-hadoop1.rb")
      there was one(amazonS3).putObject(anyString, anyString, any[InputStream], any[ObjectMetadata])
      there was noCallsTo(amazonS3)
    }

  }
}
