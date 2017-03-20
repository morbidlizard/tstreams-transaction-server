package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{BootstrapOptions, PackageTransmissionOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.curator.test.TestingServer
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration



class ServerPackageTooBigTest extends FlatSpec with Matchers {
  private val zkTestServer = new TestingServer(true)
  "Server" should "not allow client to send a message which has a size that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val packageTransmissionOptions = PackageTransmissionOptions()

    val server = new ServerBuilder().withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withBootstrapOptions(BootstrapOptions())
      .build()

    new Thread(() => {
      server.start()
    }).start()

    //It's needed to wait for a server bootstrap.
    Thread.sleep(1000)

    val client = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(requestTimeoutMs = 3000))
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()

    assertThrows[PackageTooBigException] {
      Await.result(client.putStream("Too big message", 1, Some(new String(new Array[Byte](packageTransmissionOptions.maxDataPackageSize))), 1), Duration(5, TimeUnit.SECONDS))
    }

    zkTestServer.close()
    server.shutdown()
  }
}