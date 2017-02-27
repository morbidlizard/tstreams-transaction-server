package it


import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkNoConnectionException
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.scalatest.{FlatSpec, Matchers}

class ClientServerZookeeperTest extends FlatSpec with Matchers {

  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888"))
    assertThrows[ZkNoConnectionException] {
      clientBuilder.build()
    }
  }

  "Server" should "not connect to zookeeper server that isn't running" in {
    val serverBuilder = new ServerBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8888"))
    assertThrows[ZkNoConnectionException] {
      serverBuilder.build()
    }
  }




}
