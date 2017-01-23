
import com.bwsw.netty.client.Client
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  "Client" should "not connect to zookeeper server that isn't running" in {
    val configClient = new com.bwsw.configProperties.ClientConfig(new com.bwsw.configProperties.ConfigFile("src/test/scala/it/clientIntegrationTestProperties.properties"))
    assertThrows[com.bwsw.exception.Throwables.ZkGetMasterException] {
      new Client(configClient)
    }
  }

}
