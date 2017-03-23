package benchmark.utils

import java.io.File

import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val serverBuilder = new ServerBuilder()
  private val clientBuilder = new ClientBuilder()
  private val storageOptions = serverBuilder.getStorageOptions()

  def clearDB() = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
  }

  def startTransactionServer() = {
    new Thread(() => serverBuilder.build().start()).start()
  }

  def createStream(name: String, partitions: Int) = {
    val client = clientBuilder.build()
    Await.result(client.putStream(name, partitions, None, 5), 10.seconds)

    client.shutdown()
  }

  def deleteStream(name: String) = {
    val client = clientBuilder.build()
    Await.result(client.delStream(name), 10.seconds)

    client.shutdown()
  }
}
