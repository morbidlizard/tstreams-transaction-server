package benchmark.utils

import java.io.File
import java.util.logging.LogManager

import configProperties.DB
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  def clearDB() = {
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.StreamDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionDataDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionMetaDirName))
  }

  def startTransactionServer() = {
    new Thread(new Runnable {
      LogManager.getLogManager.reset()

      override def run(): Unit = new netty.server.Server().run()
    }).start()
  }

  def createStream(name: String, partitions: Int) = {
    val client = new netty.client.Client
    Await.result(client.putStream(name, partitions, None, 5), 10.seconds)
  }

  def deleteStream(name: String) = {
    val client = new netty.client.Client
    Await.result(client.delStream(name), 10.seconds)
  }
}
