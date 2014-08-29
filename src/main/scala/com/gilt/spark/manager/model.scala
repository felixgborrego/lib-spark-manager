package com.gilt.spark.manager

import java.net.URL

object model {

  class ClusterType(val numMasters: Int, val numWorkers: Int)

  object MicroCluster extends ClusterType(1, 1)

  object NormalCluster extends ClusterType(1, 4)

  case class LocalConfig(classJob: String, localSparkHome: String, keyPair: String, keyFile: String, awsAccessKeyId: String, awsSecretKey: String, region: String)

  case class ClusterConfig(clusterType: ClusterType, autoStop: Boolean, remoteSparkHome: String = "/root/spark")

  case class SparkCluster(clusterHost: String, localConfig: LocalConfig, clusterConfig: ClusterConfig)

}
