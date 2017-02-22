package it


import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkGetMasterException
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import org.scalatest.words.DefinedWord
import org.scalatest.{FlatSpec, Matchers}

class ClientZookeeperTest extends FlatSpec with Matchers {

  //TODO client will try to connect to server forever. So this test case should be reviewed.
  "Client" should "not connect to zookeeper server that isn't running" in {
    val clientBuilder = new ClientBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = "127.0.0.1:8080"))
    assertThrows[ZkGetMasterException] {
      clientBuilder.build()
    }
  }
}
