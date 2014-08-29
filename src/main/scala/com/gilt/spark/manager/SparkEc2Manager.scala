package com.gilt.spark.manager

import java.io.{ FileInputStream, File }

import com.gilt.spark.manager.impl.{ SparkEc2SshClient, SparkEc2Script }
import com.gilt.spark.manager.model.{ ClusterConfig, SparkCluster, LocalConfig }
import fr.janalyse.ssh.{ SSHScp, ExecPart, ExecResult, SSHOptions }
import org.slf4j.LoggerFactory
import scala.concurrent._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object SparkEc2Manager {
  def apply() = new SparkEc2Manager(new SparkEc2Script, new SparkEc2SshClient)
  def apply(script: SparkEc2Script, ssh: SparkEc2SshClient) = new SparkEc2Manager(script, ssh)
}

class SparkEc2Manager(script: SparkEc2Script, ssh: SparkEc2SshClient) {

  val logger = LoggerFactory.getLogger("SparkEc2Manager")

  def createCluster(userConf: LocalConfig, clusterConfig: ClusterConfig): Future[SparkCluster] = {
    val masterIp = script.getMaster(userConf).flatMap {
      case Some(host) => future {
        logger.info(s"Cluster $host already exist!")
        host
      }
      case None => script.launch(userConf, clusterConfig)
    }

    masterIp.map(ip => new SparkCluster(ip, userConf, clusterConfig))
  }

  def getCluster(localConfig: LocalConfig) = script.getMaster(localConfig)

  def executeJob(localJar: String, classJob: String, cluster: SparkCluster) = {
    // deploy jar
    ssh.copyFileToMaster(localJar, cluster).map { remoteJar =>
      // submit task as a local jar in the remote cluster
      ssh.submitSparkJob(remoteJar, classJob, cluster)
    }
  }

  def destroy(localConfig: LocalConfig) = script.destroy(localConfig)

}

