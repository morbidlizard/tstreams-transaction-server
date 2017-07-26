package util


import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import com.bwsw.tstreamstransactionserver.netty.server.storage.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamService
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionReader
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework



class RocksReaderAndWriter(zkClient: CuratorFramework,
                           val storageOptions: StorageOptions,
                           rocksStorageOpts: RocksStorageOptions)
{

  private val rocksStorage =
    new MultiAndSingleNodeRockStorage(
      storageOptions,
      rocksStorageOpts
    )

  private val streamRepository =
    new ZookeeperStreamRepository(zkClient, s"${storageOptions.streamZookeeperDirectory}")

  private val transactionDataServiceImpl =
    new TransactionDataService(
      storageOptions,
      rocksStorageOpts,
      streamRepository
    )

  val rocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataServiceImpl
  )

  val rocksReader = new RocksReader(
    rocksStorage,
    transactionDataServiceImpl
  )

  val streamService = new StreamService(
    streamRepository
  )

  def newBatch: KeyValueDbBatch =
    rocksWriter.getNewBatch

  def closeDBAndDeleteFolder(): Unit = {
    rocksStorage.getRocksStorage.closeDatabases()
    transactionDataServiceImpl.closeTransactionDataDatabases()

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    new File(storageOptions.path).delete()
  }
}
