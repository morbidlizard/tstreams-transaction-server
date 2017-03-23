package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{BootstrapOptions, PackageTransmissionOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.curator.test.TestingServer
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClientPackageTooBigTest extends FlatSpec with Matchers {
  private val zkTestServer = new TestingServer(true)
  "Client" should "not allow to transmit amount of data that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val packageTransmissionOptions = PackageTransmissionOptions()

    val server = new ServerBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions())
      .build()

    new Thread(() => {
      server.start()
    }).start()

    val client = new ClientBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()

    Await.result(client.putStream("Message to authenticate and get the package transmission options from server", 1, Some("No description"), 1), Duration(5, TimeUnit.SECONDS))

    assertThrows[PackageTooBigException] {
      Await.result(client.putStream("Too big message", 1, Some(new String(new Array[Byte](packageTransmissionOptions.maxDataPackageSize))), 1), Duration(5, TimeUnit.SECONDS))
    }

    zkTestServer.close()
    server.shutdown()
  }
}
