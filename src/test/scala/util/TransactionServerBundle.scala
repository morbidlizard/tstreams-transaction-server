package util

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.options.ServerOptions
import org.apache.commons.io.FileUtils


final class TransactionServerBundle(val transactionServer: TransactionServer,
                                    rocksStorage: RocksStorage,
                                    transactionDataServiceImpl: TransactionDataService,
                                    val storageOptions: ServerOptions.StorageOptions,
                                    rocksOptions: ServerOptions.RocksStorageOptions)
{
  def operate(operation: TransactionServer => Unit): Unit = {
    try {
      operation(transactionServer)
    }
    catch {
      case throwable: Throwable => throw throwable
    }
    finally {
      closeDbsAndDeleteDirectories()
    }
  }

  def closeDbsAndDeleteDirectories(): Unit = {
    rocksStorage.getRocksStorage.closeDatabases()
    transactionDataServiceImpl.closeTransactionDataDatabases()
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    new File(storageOptions.path).delete()
  }
}
