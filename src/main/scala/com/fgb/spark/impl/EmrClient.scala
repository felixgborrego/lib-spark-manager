package com.fgb.spark.impl

import java.net.InetAddress

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.Cluster
import com.amazonaws.services.elasticmapreduce.model.Instance
import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.s3.{ AmazonS3Client, AmazonS3URI }
import com.fgb
import com.fgb.spark.{Logger, ClusterConf}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.concurrent.{ Await, Future, blocking }
import scala.language.postfixOps

object EmrClient {

  def apply(credentials: AWSCredentials, region: Region, log: Logger) = {
    val awsClient = new AmazonElasticMapReduceClient(credentials)
    val s3Client = new AmazonS3Client(credentials)
    awsClient.setRegion(region)
    new EmrClient(awsClient, s3Client, log)
  }
}

/**
 * Interface with EMR to manage an EMR cluster.
 */
class EmrClient(awsClient: AmazonElasticMapReduceClient, s3Client: AmazonS3Client, log: Logger) {

  val retryInterval = 20 seconds // the cluster usually takes 2 minutes to bootstrap

  def start(bootStrapScript: AmazonS3URI, conf: ClusterConf): Future[String] = Future {
    val jobRequest =
      new RunJobFlowRequest(conf.name, jobFlowConfig(conf))
        .withAmiVersion(conf.version.amiVersion)
        .withLogUri(buildLogUri(conf))
        .withBootstrapActions(bootStrap(bootStrapScript, conf), gangliaAction)
        .withTags(toTags(conf))
        .withVisibleToAllUsers(true)

    val clusterId = awsClient.runJobFlow(jobRequest).getJobFlowId
    log.logDebug(s"New cluster has been created with id: $clusterId")
    clusterId
  }

  /**
   * Find an active cluster with this name.
   */
  def find(name: String): Future[String] = {
    val summary: Future[ClusterSummary] = Future {
      import ClusterState._
      val request = new ListClustersRequest().withClusterStates(STARTING, BOOTSTRAPPING, RUNNING, WAITING)

      // get list of active clusters
      def query(request: ListClustersRequest): List[ClusterSummary] = {
        val response = awsClient.listClusters(request)
        Option(response.getMarker) match {
          case None => response.getClusters.toList
          case Some(marker) => response.getClusters.toList ::: query(request.withMarker(marker))
        }
      }

      query(request).filter(_.getName == name) match {
        case Nil => throw new NoSuchElementException(s"Cluster '$name' not found")
        case head :: tail => head
      }
    }

    summary.map(_.getId)
  }

  def describe(jobFlowId: String): Future[Cluster] = Future {
    val request = new DescribeClusterRequest().withClusterId(jobFlowId)
    val describe = awsClient.describeCluster(request)
    describe.getCluster
  }

  // Get master instance.
  def getMasterInfo(clusterId: String): Future[fgb.spark.Instance] = {
    // need to wait until the cluster is running to know what is its ip address.
    def isStarting(status: String) = status != "PROVISIONING" && status != "STARTING"
    def getMaster(): Future[(String, Instance)] = executeWhenStatus(clusterId, condition = isStarting) { status =>
      val instances = awsClient.listInstances(new ListInstancesRequest().withClusterId(clusterId).withInstanceGroupTypes(InstanceGroupType.MASTER)).getInstances.toList
      (status, instances(0))
    }

    for {
      (status, master) <- getMaster()
    } yield new fgb.spark.Instance(master.getPublicDnsName, InetAddress.getByName(master.getPublicIpAddress))
  }

  def steps(clusterId: String): Future[Seq[StepSummary]] = Future {
    def query(request: ListStepsRequest): List[StepSummary] = {
      val response = awsClient.listSteps(request)
      Option(response.getMarker) match {
        case None => response.getSteps.toList
        case Some(marker) => response.getSteps.toList ::: query(request.withMarker(marker))
      }
    }
    query(new ListStepsRequest().withClusterId(clusterId))
  }

  def findStep(clusterId: String, stepId: String): Future[Step] = Future {
    val request = new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId)
    awsClient.describeStep(request).getStep
  }

  /**
   * Add a new step to the cluster.
   * To execute a Spark job we need to submit it through v the Spark submit script.
   * Info:
   * http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-hadoop-script.html
   * http://spark.apache.org/docs/latest/submitting-applications.html
   */
  def addJob(jobName: String, submitScript: AmazonS3URI, clusterId: String): Future[String] = Future {
    log.logInfo(s"Adding job $jobName, script: ${submitScript.getKey}")
    val result = awsClient.addJobFlowSteps(buildSparkStepRequest(jobName, submitScript, clusterId))
    result.getStepIds.head
  }

  private[impl] def buildLogUri(cluster: ClusterConf) = {
    s"s3://${cluster.bucket}/jobs/${cluster.name}/logs"
  }

  def destroy(jobFlowId: String): Unit = {
    val request = new TerminateJobFlowsRequest().withJobFlowIds(jobFlowId)
    awsClient.terminateJobFlows(request)
    log.logInfo(s"Cluster $jobFlowId terminated.")
  }

  def executeWhenStatus[T](jobFlowId: String, condition: String => Boolean)(f: String => T): Future[T] = {
    describe(jobFlowId).map(_.getStatus).map { status =>
      if (condition(status.getState)) {
        f(status.getState)
      } else {
        blocking {
          // TODO add a better way to schedule the retry
          log.logInfo(s"Waiting, current status: $status, message: ${status.getStateChangeReason.getMessage}")
          Thread.sleep(retryInterval.toMillis)
          Await.result(executeWhenStatus(jobFlowId, condition)(f), Duration.Inf)
        }
      }
    }
  }

  // Spark job configuration
  private[impl] def buildSparkStepRequest(jobName: String, submitScript: AmazonS3URI, clusterId: String): AddJobFlowStepsRequest = {
    // A custom step
    val config = new HadoopJarStepConfig()
      .withJar("s3://elasticmapreduce/libs/script-runner/script-runner.jar")
      .withArgs(submitScript.toString)

    val customStep = new StepConfig(jobName, config)
    customStep.setActionOnFailure("CONTINUE")
    new AddJobFlowStepsRequest()
      .withJobFlowId(clusterId)
      .withSteps(customStep)
  }

  // Cluster configuration
  private[impl] def jobFlowConfig(config: ClusterConf) = {
    val jobFlowConf = new JobFlowInstancesConfig()
      .withKeepJobFlowAliveWhenNoSteps(true)
      .withInstanceGroups(
        new InstanceGroupConfig("MASTER", config.dimensions.masterInstanceType, config.dimensions.numberOfMasters)
          :: new InstanceGroupConfig("CORE", config.dimensions.workerInstanceType, config.dimensions.numberOfWorkers)
          :: Nil)
      .withEc2KeyName(config.keyPair.name)

    config.subnet match {
      case None => jobFlowConf
      case Some(subnet) => jobFlowConf.withEc2SubnetId(subnet)
    }
  }

  private[impl] def bootStrap(url: AmazonS3URI, conf: ClusterConf): BootstrapActionConfig =
    new BootstrapActionConfig(conf.version.distributionName, new ScriptBootstrapActionConfig().withPath(url.toString))

  private[impl] def gangliaAction() = {
    new BootstrapActionConfig("ganglia", new ScriptBootstrapActionConfig().withPath("s3://elasticmapreduce/bootstrap-actions/install-ganglia")).withName("ganglia")
  }

  private[impl] def toTags(cc: ClusterConf) = {
    def getData(cc: ClusterConf): List[(String, String)] = {
      val team = cc.teamOwner match {
        case None => Nil
        case Some(team) => ("team", team) :: Nil // used for cost management
      }

      ("cluster.spark.version", cc.version.version) ::
        ("cluster.spark.package-type", cc.version.packageType) ::
        ("cluster.s3-bucket-name", cc.bucket) :: team
    }

    getData(cc).map(e => new Tag(e._1, e._2))
  }

}
