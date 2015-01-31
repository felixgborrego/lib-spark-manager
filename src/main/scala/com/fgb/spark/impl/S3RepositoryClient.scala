package com.fgb.spark.impl

import java.io.{ InputStreamReader, ByteArrayInputStream, File }

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.model.ObjectMetadata

import com.amazonaws.services.s3.{ AmazonS3Client, AmazonS3URI }
import com.fgb.spark.{ Logger, ClusterConf }
import com.typesafe.config.{ ConfigFactory, ConfigRenderOptions }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object S3RepositoryClient {
  def apply(credentials: AWSCredentials, log: Logger) = new S3RepositoryClient(new AmazonS3Client(credentials), log)
}

/**
 * Interface with S3.
 */
class S3RepositoryClient(s3: AmazonS3Client, log: Logger) {

  /**
   * Release a new jar to the S3 repository.
   */
  def releaseJar(localFile: File, clusterName: String, jobVersion: String, cluster: ClusterConf) = Future[AmazonS3URI] {
    val path = jarS3Url(clusterName, jobVersion, cluster)
    log.logDebug(s"Deploying jar to $path")
    s3.putObject(path.getBucket, path.getKey, localFile)
    path
  }

  //release script to submit a Spark job
  def releaseScript(jobVersion: String, classJob: String, cluster: ClusterConf) = Future[AmazonS3URI] {
    val jarPath = jarS3Url(cluster.name, jobVersion, cluster)
    val scriptPath = scriptS3Url(cluster.name, jobVersion, cluster)
    val script = buildScript(classJob, jarPath, cluster)
    val metadata = new ObjectMetadata()
    val data = script.getBytes("UTF-8")
    metadata.setContentLength(data.length)
    log.logDebug(s"Deploying script to $scriptPath")
    s3.putObject(scriptPath.getBucket, scriptPath.getKey, new ByteArrayInputStream(data), metadata)
    scriptPath
  }

  def releaseBootstraps(conf: ClusterConf) = Future[AmazonS3URI] {
    val distributionName = conf.version.distributionName
    val rb = this.getClass.getResourceAsStream(s"/$distributionName/install-spark.rb")
    val url = s"s3://${conf.bucket}/jobs/${conf.name}/bootstraps/install-$distributionName.rb"
    val s3Url = new AmazonS3URI(url)
    log.logDebug(s"Deploying bootstrap to $s3Url")
    s3.putObject(s3Url.getBucket, s3Url.getKey, rb, new ObjectMetadata())
    s3Url
  }

  def releaseConf(conf: ClusterConf) = Future[ClusterConf] {
    val url = s"s3://${conf.bucket}/jobs/${conf.name}/cluster.conf"
    val config = util.toTypeSafeConfig(conf)
    val content = config.root().render(ConfigRenderOptions.defaults())
    val s3Url = new AmazonS3URI(url)
    s3.putObject(s3Url.getBucket, s3Url.getKey, new ByteArrayInputStream(content.getBytes("UTF-8")), new ObjectMetadata())
    conf
  }

  def loadConf(bucket: String, name: String) = Future[ClusterConf] {
    val url = s"s3://$bucket/jobs/$name/cluster.conf"
    val s3Url = new AmazonS3URI(url)
    val input = new InputStreamReader(s3.getObject(s3Url.getBucket, s3Url.getKey).getObjectContent, "UTF-8")
    val config = ConfigFactory.parseReader(input)
    ClusterConf(config)
  }

  def buildScript(classJob: String, jarJob: AmazonS3URI, cluster: ClusterConf): String = {
    val remoteJar = jarJob.toString
    val localJar = new File(s"/tmp/${jarJob.getKey}")
    s"""#!/bin/bash
        |#Script to submit a Spark job
        |echo Coping $remoteJar to $localJar
        |mkdir -p ${localJar.getParent}
        |hadoop fs -get $remoteJar ${localJar.getPath}
        |echo Submitting job to spark cluster spark-submit --class $classJob $localJar
        |/home/hadoop/spark/bin/spark-submit --class $classJob $localJar
        |echo Job submitted!""".stripMargin

    /* Not required with the current approach
        |#export AWS_ACCESS_KEY_ID=${'$'}(more /home/hadoop/conf/core-site.xml | sed -n 's:.*fs.s3.awsAccessKeyId</name><value>\\(.*\\)</value>.*:\\1:p')
        |#export AWS_SECRET_ACCESS_KEY=${'$'}(more /home/hadoop/conf/core-site.xml | sed -n 's:.*fs.s3.awsSecretAccessKey</name><value>\\(.*\\)</value>.*:\\1:p')
        |#aws s3 cp $remoteJar ${localJar.getPath}

     */
  }

  // path to a released jar
  private[impl] def jarS3Url(jobName: String, jobVersion: String, cluster: ClusterConf) = {
    val url = s"s3://${cluster.bucket}/jobs/$jobName/$jobName-${cluster.version.distributionName}-$jobVersion.jar"
    new AmazonS3URI(url)
  }

  // path to a released script to execute a job
  private[impl] def scriptS3Url(jobName: String, jobVersion: String, cluster: ClusterConf) = {
    val url = s"s3://${cluster.bucket}/jobs/$jobName/$jobName-${cluster.version.distributionName}-$jobVersion.sh"
    new AmazonS3URI(url)
  }

}
