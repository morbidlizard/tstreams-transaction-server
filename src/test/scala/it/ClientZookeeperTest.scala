package it


import com.bwsw.tstreamstransactionserver.exception.Throwables.ZkGetMasterException
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ZookeeperOptions}
import org.scalatest.{FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers {

  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8080"))
    assertThrows[ZkGetMasterException] {
      clientBuilder.build()
    }
  }
}
