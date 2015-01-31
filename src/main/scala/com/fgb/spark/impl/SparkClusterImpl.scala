package com.fgb.spark.impl

import java.util.NoSuchElementException

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Region
import com.amazonaws.services.elasticmapreduce.model.{ Step, StepSummary }
import com.amazonaws.services.s3.AmazonS3URI
import com.fgb.spark._
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SparkClusterImpl {
  def apply(credentials: AWSCredentials, region: Region, log: Logger) = {
    val emr = EmrClient(credentials, region, log)
    val s3 = S3RepositoryClient(credentials, log)
    new SparkClusterImpl(emr, s3, log)
  }
}

case class SparkClusterImpl(emr: EmrClient, s3: S3RepositoryClient, log: Logger) extends SparkCluster {

  /**
   * This will create a new EMR cluster.
   * conf.name is the descriptive name of the cluster.
   * Note: if there is already a cluster running with this name
   * this will reuse the cluster with the same name.
   */
  def start(conf: ClusterConf): Future[Cluster] = {
    def startNewCluster(): Future[String] = for {
      bootStrap <- s3.releaseBootstraps(conf)
      savedConf <- s3.releaseConf(conf)
      clusterId <- emr.start(bootStrap, savedConf)
    } yield clusterId

    for {
      clusterId <- emr.find(conf.name).recoverWith {
        case ex: NoSuchElementException =>
          log.logInfo(s"There is no previous cluster with name '${conf.name}', starting a new cluster...")
          startNewCluster()
      }
      instance <- emr.getMasterInfo(clusterId)
    } yield new Cluster(clusterId, conf.name, instance, conf)
  }

  /**
   * Stop the cluster.
   * By default it'll wait until there are no more jobs/steps running.
   */
  def stop(name: String, force: Boolean = false): Future[Unit] = {

    def condition(status: String): Boolean = (force, status) match {
      case (true, _) => true // force
      case (false, "RUNNING") => false // wait to finish the current step
      case _ => true // stop (cluster may be RUNNING, STARTING,...)
    }

    emr.find(name).map { clusterId =>
      emr.executeWhenStatus(clusterId, condition) { status =>
        emr.destroy(clusterId)
      }
    }
  }

  def find(name: String): Future[Cluster] = for {
    clusterId <- emr.find(name)
    instance <- emr.getMasterInfo(clusterId)
    clusterEmr <- emr.describe(clusterId)
    conf <- {
      val bucket = clusterEmr.getTags.toSeq.find(_.getKey == "cluster.s3-bucket-name").get.getValue
      s3.loadConf(bucket, name)
    }
  } yield new Cluster(clusterId, conf.name, instance, conf)

  /**
   * Submit the job to a running cluster and execute the job.
   * The future will return once the job has been accepted by the cluster, waiting until the cluster is ready to accept the job.
   * If the cluster is shutting down it'll throw an exception.
   */
  def execute(job: JobConf): Future[Job] = {
    val version = DateTime.now.toString("yyyy-MM-dd-mm-ss") // TODO add a better visioning for temporal job (no scheduled)
    val jobName = job.jar.getName.replaceAllLiterally(".jar", "")
    val jarReleased = s3.releaseJar(job.jar, jobName, version, job.cluster.conf)
    val scriptReleased: Future[AmazonS3URI] = s3.releaseScript(version, job.mainClass, job.cluster.conf)
    log.logInfo(s"Submitting new job  ${job.mainClass} to  ${job.cluster.name}")
    for {
      jar <- jarReleased
      script <- scriptReleased
      jobId <- emr.addJob(jobName, script, job.cluster.id)
    } yield new Job(jobId, job)
  }

  /**
   * Return the EMR cluster status.
   */
  def status(cluster: Cluster): Future[ClusterStatus] = for {
    info <- emr.describe(cluster.id)
    stepsSummary <- emr.steps(cluster.id)
  } yield new ClusterStatus(info.getStatus.getState, info.getStatus.getStateChangeReason.toString, stepsSummary.map(toJobStatus),cluster)

  def status(job: Job): Future[JobStatus] = for {
    step <- emr.findStep(job.conf.cluster.id, job.id)
  } yield toJobStatus(step)

  private def toJobStatus(step: Step) =
    new JobStatus(step.getId,
      step.getName,
      step.getStatus.getState,
      step.getStatus.getStateChangeReason.toString,
      Option(step.getStatus.getTimeline.getStartDateTime),
      Option(step.getStatus.getTimeline.getEndDateTime))

  private def toJobStatus(step: StepSummary) =
    new JobStatus(step.getId,
      step.getName,
      step.getStatus.getState,
      step.getStatus.getStateChangeReason.toString,
      Option(step.getStatus.getTimeline.getStartDateTime),
      Option(step.getStatus.getTimeline.getEndDateTime))

}