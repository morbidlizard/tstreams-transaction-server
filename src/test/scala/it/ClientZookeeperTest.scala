package it


import com.bwsw.tstreamstransactionserver.netty.client.Client
import org.scalatest.{FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers {

  private def clientConfig(connectionString: String): com.bwsw.tstreamstransactionserver.configProperties.ConfigMap = {
    val map = scala.collection.mutable.Map[String,String]()
    map += (("auth.login", "Aleksandr"))
    map += (("auth.timeout.connection", "5000"))
    map += (("zk.endpoints", connectionString))
    map += (("server.timeout.connection", "5000"))
    map += (("auth.password", "1444"))
    map += (("zk.timeout.session", "10000"))
    map += (("server.timeout.betweenRetries", "200"))
    map += (("client.pool", "4"))
    map += (("zk.retries.max", "5"))
    map += (("zk.prefix", "/stream"))
    map += (("zk.timeout.betweenRetries", "500"))
    map += (("auth.timeout.betweenRetries", "300"))
    map += (("auth.token.timeout.betweenRetries", "200"))
    map += (("auth.token.timeout.connection", "5000"))
    map += (("zk.timeout.connection", "10000"))
    new com.bwsw.tstreamstransactionserver.configProperties.ConfigMap(map.toMap)
  }


  "Client" should "not connect to zookeeper server that isn't running" in {
    val configClient = new com.bwsw.tstreamstransactionserver.configProperties.ClientConfig(clientConfig("127.0.0.1:8080"))
    assertThrows[com.bwsw.tstreamstransactionserver.exception.Throwables.ZkGetMasterException] {
      new Client(configClient)
    }
  }
}
