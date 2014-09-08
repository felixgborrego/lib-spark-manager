package com.gilt.spark.manager.impl

import java.io.{ByteArrayInputStream, BufferedOutputStream, File}
import java.lang
import java.net.URL

import com.gilt.spark.manager.model.{ ClusterConfig, LocalConfig, ClusterType }
import org.slf4j.LoggerFactory



import scala.sys.process._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import scala.collection.JavaConversions._

/**
 * Bridge with the spark-ec2 script.
 * Info: https://github.com/apache/spark/blob/master/docs/ec2-scripts.md
 */
class SparkEc2Script {
  val logger = LoggerFactory.getLogger("SparkEc2Script")

  // Seconds to wait for nodes to start
  val waitNodes = 250

  val clusterName = "Spark"

  /**
   * Launch cluster.
   * Return the public name of the server: ex: ec2-54-77-123-64.eu-west-1.compute.amazonaws.com
   */
  def launch(config: LocalConfig, clusterConfig: ClusterConfig): Future[String] = Future {
    logger.info("Creating cluster... (this operation will take a couple of minutes)")

    val output: Stream[String] = executeCmd(s"launch $clusterName", config, Some(clusterConfig))

    val done = output.last == "Done!"
    if (done) {
      val url = output.filter(_.startsWith("Spark standalone cluster started at")).head.split(" ").last
      new URL(url).getHost
    } else {
      throw new Exception("Unable launch the cluster")
    }
  }

  /**
   * Get cluster's ip.
   *  Return the public name of the server: ex: ec2-54-77-123-64.eu-west-1.compute.amazonaws.com
   */
  def getMaster(config: LocalConfig): Future[Option[String]] = Future[Option[String]] {
    logger.info("Getting cluster public ip... (this operation will take a couple of minutes)")

    val output: Stream[String] = executeCmd(s"get-master $clusterName", config)

    val ip = output.last
    val done = ip.endsWith("amazonaws.com")
    if (done) {
      Some(ip)
    } else None
  }

  /**
   * Run ./spark-ec2 destroy <cluster-name>.
   */
  def destroy(config: LocalConfig) = {
    logger.info("Destroy cluster...")
    executeCmd(s"destroy $clusterName", config)
  }

  private def executeCmd(ec2Cmd: String, config: LocalConfig, cluster: Option[ClusterConfig] = None): Stream[String] = {
    val params = cluster match {
      case None => s"-r ${config.region} -k ${config.keyPair} -i ${config.keyFile}"
      case Some(c) => s"-r ${config.region} -k ${config.keyPair} -i ${config.keyFile} -s ${c.clusterType.numMasters} --worker-instances ${c.clusterType.numWorkers} -w $waitNodes"
    }
    val cmd = s"${config.localSparkHome}/ec2/spark-ec2 $params $ec2Cmd"

    logger.debug(s"running: $cmd")

    var pb = new lang.ProcessBuilder(cmd.split(" ").toList)
    val env = pb.environment()
    pb.environment().put("AWS_ACCESS_KEY_ID", config.awsAccessKeyId)
    pb.environment().put("AWS_DEFAULT_REGION", config.region)
    pb.environment().put("AWS_SECRET_ACCESS_KEY", config.awsSecretKey)

    val processLogger = ProcessLogger(
      (o: String) => println("out " + o),
      (e: String) => println("err " + e))


    def yes = new ByteArrayInputStream("y\n".getBytes)

    val pbScala = pb #< yes

    val output=  pbScala lines_!


    
    output.foreach(line =>
      logger.debug(s"out>\t$line"))

    output
  }

}
