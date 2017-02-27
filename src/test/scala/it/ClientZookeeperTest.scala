package it


import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.scalatest.{FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers {

  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888"))
    assertThrows[ZkGetMasterException] {
      clientBuilder.build()
    }
  }
}
