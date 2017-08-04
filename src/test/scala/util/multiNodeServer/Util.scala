package util.multiNodeServer

import java.io.File
import java.nio.file.Files

import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer, multiNode}
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.curator.framework.CuratorFramework
import util.TransactionServerBundle

object Util {
  private def testStorageOptions(dbPath: File) = {
    StorageOptions().copy(
      path = dbPath.getPath,
      streamZookeeperDirectory = s"/$uuid"
    )
  }

  private def tempFolder() = {
    Files.createTempDirectory("tts").toFile
  }

  def uuid: String = java.util.UUID.randomUUID.toString

  def getTransactionServerBundle(zkClient: CuratorFramework): MultiNodeBundle = {
    val dbPath = tempFolder()

    val storageOptions =
      testStorageOptions(dbPath)

    val rocksStorageOptions =
      RocksStorageOptions()

    val rocksStorage =
      new MultiAndSingleNodeRockStorage(
        storageOptions,
        rocksStorageOptions
      )

    val zkStreamRepository =
      new ZookeeperStreamRepository(
        zkClient,
        storageOptions.streamZookeeperDirectory
      )

    val transactionDataService =
      new TransactionDataService(
        storageOptions,
        rocksStorageOptions,
        zkStreamRepository
      )

    val rocksWriter =
      new RocksWriter(
        rocksStorage,
        transactionDataService
      )

    val rocksReader =
      new RocksReader(
        rocksStorage,
        transactionDataService
      )

    val transactionServer =
      new TransactionServer(
        zkStreamRepository,
        rocksWriter,
        rocksReader
      )

    val multiNodeCommitLogService =
      new multiNode.commitLogService.CommitLogService(
        rocksStorage.getRocksStorage
      )

    new MultiNodeBundle(
      transactionServer,
      rocksWriter,
      rocksReader,
      multiNodeCommitLogService,
      rocksStorage,
      transactionDataService,
      storageOptions,
      rocksStorageOptions
    )
  }

}
