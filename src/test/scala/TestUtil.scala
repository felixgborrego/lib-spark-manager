package com.fgb.spark.impl

import java.io.{ File, InputStream }
import java.net.InetAddress

import com.amazonaws.AmazonClientException
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.s3.{ AmazonS3URI, AmazonS3Client }
import com.amazonaws.services.s3.model.{ PutObjectResult, ObjectMetadata }
import com.fgb.spark.{ClusterConf, Instance}
import com.fgb.spark.impl.{S3RepositoryClient, EmrClient, util}
import util.Slf4jLogger
import com.typesafe.config.ConfigFactory
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification

import scala.concurrent.Future

trait TestUtil extends Specification with Mockito {
  lazy val clusterConfig: ClusterConf = {
    val conf = ConfigFactory.load("test-spark-cluster.conf")
    ClusterConf(conf)
  }

  def getMockS3: S3RepositoryClient = {
    val s3 = smartMock[S3RepositoryClient]
    s3.releaseBootstraps(any[ClusterConf]) returns Future { new AmazonS3URI("s3://bucket/clusterId/bootstrap") }
    s3.releaseConf(any[ClusterConf]) returns Future { clusterConfig }

    s3
  }

  def getMockEmr: EmrClient = {
    val emr = smartMock[EmrClient]

    emr.find(anyString) answers { request =>
      Future { if (request == "newClusterName") throw new NoSuchElementException("not found") else "clusterId" }
    }

    emr.start(any[AmazonS3URI], any[ClusterConf]) returns { Future { "clusterId" } }
    emr.getMasterInfo(anyString) returns Future { Instance("test.com", InetAddress.getByName("1.1.1.1")) }

    emr
  }

  def getEmrMocks: (AmazonElasticMapReduceClient, AmazonS3Client, EmrClient) = {
    val logger = new Slf4jLogger
    val amazonEmr = mock[AmazonElasticMapReduceClient]
    val amazonS3 = mock[AmazonS3Client]

    amazonEmr.runJobFlow(any[RunJobFlowRequest]) returns new RunJobFlowResult().withJobFlowId("id-1")
    amazonS3.putObject(anyString, anyString, any[InputStream], any[ObjectMetadata]) returns new PutObjectResult

    amazonEmr.listSteps(any[ListStepsRequest]).answers { request =>
      request match {
        case request: ListStepsRequest => Some(request.getMarker) match {
          case Some(mark) if mark == "markToStep2" => listSteps2 // second request.
          case _ => listSteps1 // first request
        }
      }
    }
    //mock to test pagination behaviour
    amazonEmr.listClusters(any[ListClustersRequest]) answers { req =>
      req match {
        case r: ListClustersRequest => Option(r.getMarker) match {
          case Some("next-marker-id") => new ListClustersResult().withClusters(new ClusterSummary().withId("id2").withName("clusterName2"))
          case None | Some(_) => new ListClustersResult().withMarker("next-marker-id").withClusters(new ClusterSummary().withId("id1").withName("name1"))
        }
        case _ => new ListClustersResult()
      }
    }

    amazonEmr.describeCluster(any[DescribeClusterRequest]) returns status
    (amazonEmr, amazonS3, new EmrClient(amazonEmr, amazonS3, logger))
  }

  def getS3Mocks: (AmazonS3Client, S3RepositoryClient) = {
    val logger = new Slf4jLogger
    lazy val amazonS3 = mock[AmazonS3Client]
    amazonS3.putObject(anyString, anyString, any[File]) answers { bucketName =>
      if (bucketName == "invalidBucket") throw new AmazonClientException("invalid bucket") else new PutObjectResult()
    }
    (amazonS3, new S3RepositoryClient(amazonS3, logger))
  }

  lazy val status = new DescribeClusterResult().withCluster(new Cluster().withStatus(new ClusterStatus().withState("STARTING")))
  lazy val listSteps1 = new ListStepsResult().withSteps(new StepSummary().withId("id1").withName("step1")).withMarker("markToStep2")
  lazy val listSteps2 = new ListStepsResult().withSteps(new StepSummary().withId("id2").withName("step2"))

}
