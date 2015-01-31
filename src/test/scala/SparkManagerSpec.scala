import com.amazonaws.services.s3.AmazonS3URI
import com.fgb.spark.ClusterConf
import com.fgb.spark.impl.{ SparkClusterImpl, util }
import util.Slf4jLogger
import com.fgb.spark.impl.TestUtil

import scala.concurrent.duration.Duration
import scala.concurrent.{ Future, Await }

class SparkManagerSpec extends TestUtil {

  "SparkManager" should {

    "start a new Cluster if there is not any other cluster with the same name running" in {
      val s3 = getMockS3
      val emr = getMockEmr
      val client = new SparkClusterImpl(emr, s3, new Slf4jLogger)
      val cluster = client.start(clusterConfig.copy(name = "newClusterName"))

      Await.result(cluster, Duration.Inf).id must beEqualTo("clusterId")
      there was one(emr).find(anyString)
      there was one(emr).start(any[AmazonS3URI], any[ClusterConf])
      there was one(emr).getMasterInfo(anyString)
      there was noMoreCallsTo(emr)
    }

    "reuse an existing Cluster running with the same name" in {
      val s3 = getMockS3
      val emr = getMockEmr
      val manager = new SparkClusterImpl(emr, s3, new Slf4jLogger)

      val cluster = manager.start(clusterConfig.copy(name = "clusterName2"))
      Await.result(cluster, Duration.Inf).id must beEqualTo("clusterId")
      there was one(emr).find(anyString)
      there was no(emr).start(any[AmazonS3URI], any[ClusterConf])
      there was one(emr).getMasterInfo(anyString)
      there was noMoreCallsTo(emr)

    }
  }

}

