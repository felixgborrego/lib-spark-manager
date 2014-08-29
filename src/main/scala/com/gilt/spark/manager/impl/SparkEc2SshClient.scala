package com.gilt.spark.manager.impl

import java.io.File

import com.gilt.spark.manager.model.{ SparkCluster, ClusterConfig, LocalConfig }
import fr.janalyse.ssh.SSHOptions
import fr.janalyse.ssh.SSH
import org.slf4j.LoggerFactory
import scala.concurrent._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SparkEc2SshClient {

  val logger = LoggerFactory.getLogger("SparkEc2SshClient")

  def copyFileToMaster(localFile: String, clusterInfo: SparkCluster): Future[String] = future {

    val sparkHome = clusterInfo.clusterConfig.remoteSparkHome
    val fileName = new File(localFile).getName
    val remoteFile = s"$sparkHome/$fileName"

    val opts = sshopts(clusterInfo)
    SSH.once(opts) { ssh =>
      ssh.scp { scp =>

        logger.info(s"Sending file $localFile to $remoteFile ")
        scp.send(localFile, remoteFile)
      }
    }
    remoteFile
  }

  def submitSparkJob(remoteJar: String, classJob: String, clusterInfo: SparkCluster) = {

    val sparkHome = clusterInfo.clusterConfig.remoteSparkHome
    val sparkPathBin = s"$sparkHome/bin"

    val sparkSubmit = s"$sparkPathBin/spark-submit $remoteJar --class $classJob"
    logger.info("Submiting task to Spark Cluster")
    logger.info(sparkSubmit)

    val opts = sshopts(clusterInfo)
    SSH.once(opts) { ssh =>
      ssh.shell { sh => // For higher performances
        val hostname = sh.executeAndTrim("hostname")
        logger.info(s"Private master ip $hostname")
        val output = sh.execute(sparkSubmit)
        output.split("\n").foreach { line =>
          logger.info(s"[Remote Cluster output] $line")
        }
      }
    }
  }

  private def sshopts(clusterInfo: SparkCluster) = {
    val sshPathFile = clusterInfo.localConfig.keyFile
    val sshDir = new File(sshPathFile).getParent
    val sshKeyFile = new File(sshPathFile).getName
    SSHOptions(host = clusterInfo.clusterHost, sshKeyFile = Some(sshKeyFile), sshUserDir = sshDir, username = "root")
  }
}
