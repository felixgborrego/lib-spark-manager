package com.fgb.spark

import java.util.Date

import com.amazonaws.auth.AWSCredentials
import com.fgb.spark.impl
import com.fgb.spark.impl.SparkClusterImpl

import scala.concurrent.Future
import java.io.File
import com.amazonaws.regions.{ Regions, Region }
import java.net.InetAddress
import com.typesafe.config.{ Config, ConfigFactory }
import impl.util.{ Slf4jLogger, RichConfig }

object SparkCluster {
  def apply(credentials: AWSCredentials, region: Region, log: Logger = new Slf4jLogger): SparkCluster = SparkClusterImpl(credentials, region, log)
}

trait SparkCluster {
  def start(conf: ClusterConf): Future[Cluster]
  def stop(name: String, force: Boolean = false): Future[Unit]
  def find(name: String): Future[Cluster]
  def execute(conf: JobConf): Future[Job]
  def status(cluster: Cluster): Future[ClusterStatus]
  def status(job: Job): Future[JobStatus]
}

case class Cluster(id: String, name: String, master: Instance, conf: ClusterConf)
case class Instance(publicDNSName: String, publicIpAddress: InetAddress)

object ClusterConf {
  def apply(config: Config): ClusterConf = {

    val confWithDefault = config.withFallback(ConfigFactory.load("default-spark-cluster.conf"))
    val name = confWithDefault.getString("cluster.name")
    val sparkVersion = confWithDefault.getString("cluster.spark.version")
    val sparkPackage = confWithDefault.getString("cluster.spark.package-type")
    val version = Version(sparkPackage, sparkVersion)

    val numberOfMasters = confWithDefault.getInt("cluster.spark.master.instances")
    val numberOfWorkers = confWithDefault.getInt("cluster.spark.worker.instances")
    val masterInstanceType = confWithDefault.getString("cluster.spark.master.type")
    val workerInstanceType = confWithDefault.getString("cluster.spark.worker.type")
    val dimensions = Dimensions(numberOfMasters, numberOfWorkers, masterInstanceType, workerInstanceType)

    val subnet = confWithDefault.getOptionalString("cluster.ec2.subnet")
    val teamOwner = confWithDefault.getOptionalString("cluster.team-owner")

    val bucketName = confWithDefault.getString("cluster.s3-bucket-name")
    val keyPemFile = new File(confWithDefault.getString("cluster.ec2.key-pair"))
    val keyName = confWithDefault.getString("cluster.ec2.key-name")
    val keyPair = KeyPair(keyName, keyPemFile)

    ClusterConf(name, version, dimensions, keyPair, bucketName, subnet, teamOwner)
  }
}

case class ClusterConf(name: String, version: Version, dimensions: Dimensions, keyPair: KeyPair, bucket: String, subnet: Option[String], teamOwner: Option[String])
case class KeyPair(name: String, pemFile: File)
case class Dimensions(numberOfMasters: Int, numberOfWorkers: Int, masterInstanceType: String, workerInstanceType: String)

sealed trait Version {
  val packageType: String
  val version: String
  val amiVersion: String
  def distributionName = s"spark-$version-bin-$packageType"
}

object Spark_1_1_0_Hadoop_1 extends Version {
  val packageType = "hadoop1"
  val version = "1.1.0"
  val amiVersion = "2.4.8"
}

object Spark_1_1_0_Hadoop_2_4 extends Version {
  val packageType = "hadoop2.4"
  val version = "1.1.0"
  val amiVersion = "3.2.1"
}

object Spark_1_1_1_Hadoop_2_4 extends Version {
  val packageType = "hadoop2.4"
  val version = "1.1.1"
  val amiVersion = "3.2.1"
}

// Spark version supported.
object Version {
  def apply(packageType: String, version: String) = (packageType, version) match {
    case ("hadoop1", "1.1.0") => Spark_1_1_0_Hadoop_1
    case ("hadoop2.4", "1.1.0") => Spark_1_1_0_Hadoop_2_4
    case ("hadoop2.4", "1.1.1") => Spark_1_1_1_Hadoop_2_4
    case _ => throw new NoSuchElementException(s"Spark version unsupported '$packageType-$version' ")
  }
}

case class ClusterStatus(state: String, message: String, jobs: Seq[JobStatus], cluster: Cluster)
case class JobStatus(id: String, name: String, state: String, message: String, creation: Option[Date], end: Option[Date])
case class JobConf(mainClass: String, jar: File, cluster: Cluster)
case class Job(id: String, conf: JobConf)

trait Logger {
  def logError(msg: => String)
  def logWarn(msg: => String)
  def logInfo(msg: => String)
  def logDebug(msg: => String)
}