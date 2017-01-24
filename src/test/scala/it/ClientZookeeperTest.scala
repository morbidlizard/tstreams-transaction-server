package it


import com.bwsw.tstreamstransactionserver.netty.client.Client
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers {
  "Client" should "not connect to zookeeper server that isn't running" in {
    val configClient = new com.bwsw.tstreamstransactionserver.configProperties.ClientConfig(new com.bwsw.tstreamstransactionserver.configProperties.ConfigFile("src/test/scala/it/clientIntegrationTestProperties.properties"))
    assertThrows[com.bwsw.tstreamstransactionserver.exception.Throwables.ZkGetMasterException] {
      new Client(configClient)
    }
  }
}
