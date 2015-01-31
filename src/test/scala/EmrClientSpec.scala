package com.fgb.spark.impl

import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.s3.{ AmazonS3URI }
import com.fgb.spark.Dimensions
import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class EmrClientSpec extends TestUtil {

  "EmrClient" should {

    "start a new Cluster" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      val urlBootStrap = mock[AmazonS3URI]
      val clusterId = emrClient.start(urlBootStrap, clusterConfig)

      clusterId must beEqualTo("id-1").await
      there was one(amazonEmr).runJobFlow(any[RunJobFlowRequest])

    }

    "setup the excepted log path for the cluster" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      emrClient.buildLogUri(clusterConfig) must beEqualTo("s3://bucket/jobs/test/logs")
      there was noCallsTo(amazonS3)
      there was noCallsTo(amazonEmr)
    }

    "return a valid status cluster" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      val expected = listSteps1.getSteps.toSeq ++ listSteps2.getSteps.toSeq
      Await.result(emrClient.steps("clusterId"), Duration.Inf) must beEqualTo(expected)
      there was two(amazonEmr).listSteps(any[ListStepsRequest])
      there was noCallsTo(amazonS3)
    }

    "configure the EMR cluster" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks

      val jobFlowConfig = emrClient.jobFlowConfig(clusterConfig)

      jobFlowConfig.getKeepJobFlowAliveWhenNoSteps must beEqualTo(true)
      jobFlowConfig.getInstanceGroups.toList(0) must beEqualTo(new InstanceGroupConfig("MASTER", "m1.medium", 1))
      jobFlowConfig.getInstanceGroups.toList(1) must beEqualTo(new InstanceGroupConfig("CORE", "m1.medium", 1))

      val dimensions = Dimensions(numberOfMasters = 1, numberOfWorkers = 2, masterInstanceType = "m3.large", workerInstanceType = "m3.2xlarge")
      val config = clusterConfig.copy(dimensions = dimensions)
      val jobFlowConfig2 = emrClient.jobFlowConfig(config)
      jobFlowConfig2.getInstanceGroups.toList(0) must beEqualTo(new InstanceGroupConfig("MASTER", "m3.large", 1))
      jobFlowConfig2.getInstanceGroups.toList(1) must beEqualTo(new InstanceGroupConfig("CORE", "m3.2xlarge", 2))
      there was noCallsTo(amazonS3)
      there was noCallsTo(amazonEmr)
    }

    "configure a new step to the EMR cluster" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      val script = new AmazonS3URI("s3://bucketName/scripts/script.sh")
      val request = emrClient.buildSparkStepRequest("jobName", script, "id-1")

      request.getJobFlowId must beEqualTo("id-1")
      val step = request.getSteps.toList(0)
      step.getName must beEqualTo("jobName")
      step.getHadoopJarStep.getJar must beEqualTo("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
      step.getHadoopJarStep.getArgs.toList(0) must beEqualTo("s3://bucketName/scripts/script.sh")
      step.getActionOnFailure must beEqualTo("CONTINUE")
      there was noCallsTo(amazonS3)
      there was noCallsTo(amazonEmr)
    }

    "save the metadata into tags" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      val tags = emrClient.toTags(clusterConfig)

      tags.map(_.getKey) must beEqualTo("cluster.spark.version" :: "cluster.spark.package-type" :: "cluster.s3-bucket-name" :: Nil)

      val tagsWithTeam = emrClient.toTags(clusterConfig.copy(teamOwner = Some("rtd")))
      tagsWithTeam.map(_.getKey) must beEqualTo("cluster.spark.version" :: "cluster.spark.package-type" :: "cluster.s3-bucket-name" :: "team" :: Nil)
      there was noCallsTo(amazonS3)
      there was noCallsTo(amazonEmr)
    }

    "return an active cluster by name" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      val clusterId = Await.result(emrClient.find("clusterName2"), Duration.Inf)
      clusterId must beEqualTo("id2")
      there was noCallsTo(amazonS3)
      there was two(amazonEmr).listClusters(any[ListClustersRequest])
      there was noCallsTo(amazonS3)
    }

    "return nothing if there is no an active cluster by name" in {
      val (amazonEmr, amazonS3, emrClient) = getEmrMocks
      emrClient.find("unknown job") must throwAn[NoSuchElementException].await()
      there was noCallsTo(amazonS3)
      there was two(amazonEmr).listClusters(any[ListClustersRequest])
    }

  }

}

