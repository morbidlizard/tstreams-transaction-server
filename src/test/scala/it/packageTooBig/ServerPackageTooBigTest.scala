package it.packageTooBig

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{BootstrapOptions, StorageOptions, TransportOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration



class ServerPackageTooBigTest extends FlatSpec with Matchers {
  private val serverStorageOptions = StorageOptions(path = "/tmp")
  "Server" should "not allow client to send a message which has a size that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))
    val zkTestServer = new TestingServer(true)

    val packageTransmissionOptions = TransportOptions(maxMetadataPackageSize = 1000)

    val server = new ServerBuilder()
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString))
      .withPackageTransmissionOptions(packageTransmissionOptions)
      .withBootstrapOptions(BootstrapOptions())
      .build()

    new Thread(() => {
      server.start()
    }).start()

    val client = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(requestTimeoutMs = 3000))
      .withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()

    assertThrows[PackageTooBigException] {
      Await.result(client.putStream("Too big message", 1, Some(new String(new Array[Byte](packageTransmissionOptions.maxMetadataPackageSize))), 1), Duration(5, TimeUnit.SECONDS))
    }

    client.shutdown()
    zkTestServer.close()
    server.shutdown()
  }
}