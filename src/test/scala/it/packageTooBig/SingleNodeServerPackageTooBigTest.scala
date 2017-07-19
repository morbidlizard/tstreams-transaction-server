package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.client.InetClientProxy
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.TransportOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, SingleNodeServerBuilder}
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration.Duration



class SingleNodeServerPackageTooBigTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {


  private val packageTransmissionOptions = TransportOptions(maxMetadataPackageSize = 1000)

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withPackageTransmissionOptions(packageTransmissionOptions)

  private lazy val clientBuilder = new ClientBuilder()

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  "Server" should "not allow client to send a message which has a size that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      assertThrows[PackageTooBigException] {
        Await.result(client.putStream(
          "Too big message",
          1,
          Some(new String(new Array[Byte](packageTransmissionOptions.maxMetadataPackageSize))), 1
        ), Duration(5, TimeUnit.SECONDS))
      }
    }
  }
}