package benchmark.utils

import java.io.File
import java.util.logging.LogManager

import com.bwsw.tstreamstransactionserver.configProperties.{ConfigFile, ServerConfig}
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.Server
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val configServer = new ServerConfig(new ConfigFile("src/main/resources/serverProperties.properties"))

  def clearDB() = {
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbStreamDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionDataDirName))
    FileUtils.deleteDirectory(new File(configServer.dbPath + "/" + configServer.dbTransactionMetaDirName))
  }

  def startTransactionServer() = {
    new Thread(new Runnable {
      LogManager.getLogManager.reset()

      override def run(): Unit = new Server(configServer).start()
    }).start()
  }

  def createStream(name: String, partitions: Int) = {
    val client = new Client
    Await.result(client.putStream(name, partitions, None, 5), 10.seconds)

    client.shutdown()
  }

  def deleteStream(name: String) = {
    val client = new Client
    Await.result(client.delStream(name), 10.seconds)

    client.shutdown()
  }
}
