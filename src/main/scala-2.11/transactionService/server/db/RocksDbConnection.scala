package transactionService.server.db

import java.io.Closeable

import configProperties.DB
import org.rocksdb._


class RocksDbConnection(ttl: Int = -1) extends Closeable{
  val client = TtlDB.open(new Options().setCreateIfMissing(true),RocksDbConnection.path.getAbsolutePath, ttl, false)
  override def close(): Unit = client.close()
}

object RocksDbConnection {
  val path = transactionService.io.FileUtils.createDirectory(DB.TransactionDataDirName)
}
