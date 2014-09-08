package com.gilt

import com.gilt.spark.manager.SparkEc2Manager
import com.gilt.spark.manager.impl.{ SparkEc2SshClient, SparkEc2Script }
import com.gilt.spark.manager.model.{ SparkCluster, LocalConfig, ClusterConfig }
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.gilt.spark.manager.utils._
import scala.concurrent.Future
import org.scalatest.WordSpec

class SparkEc2ManagerSpec extends WordSpec with MockitoSugar with Matchers {

  val host = "ip"

  "Spark Ec2 Manager" should {

    "check if there is already a Spark cluster running" in {
      val script = mock[SparkEc2Script]
      val ssh = mock[SparkEc2SshClient]
      val localConfig = mock[LocalConfig]
      val clusterConfig = mock[ClusterConfig]

      when(script.getMaster(localConfig)).thenReturn(Future.successful(Some(host)))

      val manager = SparkEc2Manager(script, ssh)
      val existTrue = manager.getCluster(localConfig).await
      existTrue shouldEqual Some(host)

      when(script.getMaster(localConfig)).thenReturn(Future.successful(None))
      val existFalse = manager.getCluster(localConfig).await
      existFalse shouldEqual None

      verify(script, never()).launch(localConfig, clusterConfig)
    }

    "return the master ip if there is already a Spark cluster running" in {
      val script = mock[SparkEc2Script]
      val ssh = mock[SparkEc2SshClient]
      val localConfig = mock[LocalConfig]
      val clusterConfig = mock[ClusterConfig]

      when(script.getMaster(localConfig)).thenReturn(Future.successful(Some(host)))

      val manager = SparkEc2Manager(script, ssh)
      val clusterInfo = manager.createCluster(localConfig, clusterConfig).await
      clusterInfo.clusterHost shouldEqual host
      verify(script, never()).launch(localConfig, clusterConfig)
    }

    "lunch a new cluster and the master ip if there is not a Spark cluster running" in {
      val script = mock[SparkEc2Script]
      val ssh = mock[SparkEc2SshClient]
      val localConfig = mock[LocalConfig]
      val clusterConfig = mock[ClusterConfig]

      when(script.getMaster(localConfig)).thenReturn(Future.successful(None))
      when(script.launch(localConfig, clusterConfig)).thenReturn(Future.successful(host))

      val manager = SparkEc2Manager(script, ssh)
      val clusterInfo = manager.createCluster(localConfig, clusterConfig).await
      clusterInfo.clusterHost shouldEqual host

      verify(script, atLeastOnce()).getMaster(localConfig)
      verify(script, atLeastOnce()).launch(localConfig, clusterConfig)
    }

    "submit a job to an existent Spark cluster running" in {
      val script = mock[SparkEc2Script]
      val ssh = mock[SparkEc2SshClient]
      val localConfig = mock[LocalConfig]
      val clusterConfig = mock[ClusterConfig]
      val sparkCluster = mock[SparkCluster]

      when(ssh.copyFileToMaster("/local.jar", sparkCluster)).thenReturn(Future.successful("/remote.jar"))
      when(script.getMaster(localConfig)).thenReturn(Future.successful(Some(host)))

      val manager = SparkEc2Manager(script, ssh)
      manager.executeJob("/local.jar", "testclass", sparkCluster)

      verify(ssh, atLeastOnce()).copyFileToMaster("/local.jar", sparkCluster)
      verify(ssh, atLeastOnce()).submitSparkJob("/remote.jar", "testclass", sparkCluster)
    }

  }
}
