package transactionService.server.db

import java.io.Closeable

import configProperties.DB
import org.rocksdb._

class RocksDbConnection(ttl: Int = -1) extends Closeable{
  private val client = TtlDB.open(new Options().setCreateIfMissing(true),RocksDbConnection.path.getAbsolutePath, ttl, false)
  private val batch  = new WriteBatch()
  def put(key: Array[Byte], data: Array[Byte]) = {
    batch.put(key,data)
  }
  def get(key: Array[Byte]) = client.get(key)
  def remove(key: Array[Byte]) = batch.remove(key)
  def write(): Boolean = {
    Option(client.write(new WriteOptions(), batch)) match {
      case Some(_) => true
      case None => false
    }
  }
  def iterator = client.newIterator()
  override def close(): Unit = {
    batch.close()
    client.close()
  }
}

private object RocksDbConnection {
  val path = transactionService.io.FileUtils.createDirectory(DB.TransactionDataDirName)
}
