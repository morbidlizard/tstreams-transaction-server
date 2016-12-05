package benchmark

import java.io.File
import java.util.logging.LogManager

import configProperties.DB
import org.apache.commons.io.FileUtils
import transactionZookeeperService.TransactionZooKeeperServer

trait Installer {
  def clearDB() = {
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.StreamDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionDataDirName))
    FileUtils.deleteDirectory(new File(DB.PathToDatabases + "/" + DB.TransactionMetaDirName))
  }

  def startAuthServer() = {
    new Thread(new Runnable {
      LogManager.getLogManager.reset()

      override def run(): Unit = authService.AuthServer.main(Array())
    }).start()
  }

  def startTransactionServer() = {
    new Thread(new Runnable {
      LogManager.getLogManager.reset()

      override def run(): Unit = TransactionZooKeeperServer.main(Array())
    }).start()
  }
}
