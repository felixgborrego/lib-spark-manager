package com.fgb.spark.impl

import com.fgb.spark.{Logger, ClusterConf}
import com.typesafe.config.{ ConfigValueFactory, ConfigFactory, Config }
import org.slf4j.LoggerFactory

object util {
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = if (underlying.hasPath(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }

    def add(key: String, value: Option[String]): Config = value match {
      case Some(v) => underlying.withValue(key, ConfigValueFactory.fromAnyRef(value))
      case None => underlying
    }
    def add(key: String, value: AnyRef): Config = underlying.withValue(key, ConfigValueFactory.fromAnyRef(value))
    def add(key: String, value: Int): Config = underlying.withValue(key, ConfigValueFactory.fromAnyRef(value))
  }

  class Slf4jLogger extends Logger {

    // Make the log field transient so that objects with Logging can
    // be serialized and used on another machine
    @transient private var log_ : org.slf4j.Logger = null

    // Method to get or create the logger for this object
    protected def log: org.slf4j.Logger = {
      if (log_ == null) {
        val logName = this.getClass.getName.stripSuffix("$")
        log_ = LoggerFactory.getLogger(logName)
      }
      log_
    }

    override def logError(msg: => String): Unit = log.error(msg)

    override def logDebug(msg: => String): Unit = log.debug(msg)

    override def logInfo(msg: => String): Unit = log.info(msg)

    override def logWarn(msg: => String): Unit = log.warn(msg)
  }

  def toTypeSafeConfig(conf: ClusterConf): Config = ConfigFactory.empty()
    .add("cluster.name", conf.name)
    .add("cluster.spark.version", conf.version.version)
    .add("cluster.spark.package-type", conf.version.packageType)
    .add("cluster.spark.master.instances", conf.dimensions.numberOfMasters)
    .add("cluster.spark.worker.instances", conf.dimensions.numberOfWorkers)
    .add("cluster.spark.master.type", conf.dimensions.masterInstanceType)
    .add("cluster.spark.worker.type", conf.dimensions.workerInstanceType)
    .add("cluster.spark.worker.type", conf.dimensions.workerInstanceType)
    .add("cluster.ec2.subnet", conf.subnet)
    .add("cluster.s3-bucket-name", conf.bucket)
    .add("cluster.ec2.key-name", conf.keyPair.name)
    .add("cluster.ec2.key-pair", conf.keyPair.pemFile.toString)

}
