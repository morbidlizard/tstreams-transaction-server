package benchmark.utils

import java.io.File

import com.bwsw.tstreamstransactionserver.options.ServerOptions.CommitLogOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val serverBuilder = new ServerBuilder()
  private val clientBuilder = new ClientBuilder()
  private val storageOptions = serverBuilder.getStorageOptions

  def clearDB() = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
  }

  def startTransactionServer() = {
    new Thread(() =>
      serverBuilder
        .withCommitLogOptions(CommitLogOptions(commitLogCloseDelayMs = 1000))
        .build().start()
    ).start()
  }

  def createStream(name: String, partitions: Int): Int = {
    val client = clientBuilder.build()
    val streamID = Await.result(client.putStream(name, partitions, None, 5), 10.seconds)
    client.shutdown()
    streamID
  }

  def deleteStream(id: Int) = {
    val client = clientBuilder.build()
    Await.result(client.delStream(id), 10.seconds)

    client.shutdown()
  }
}
