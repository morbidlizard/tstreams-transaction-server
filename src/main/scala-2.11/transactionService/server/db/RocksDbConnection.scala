package transactionService.server.db

import java.io.Closeable

import configProperties.DB
import org.rocksdb._

class RocksDbConnection(name: String, ttl: Int = -1) extends Closeable {
  RocksDB.loadLibrary()

  private lazy val client =  {
    val path = transactionService.io.FileUtils.createDirectory(s"${RocksDbConnection.rocksDBStoragesPath}/$name").getAbsolutePath
    TtlDB.open(new Options().setCreateIfMissing(true), path, ttl, false)
  }

  def get(key: Array[Byte]) = client.get(key)

  def iterator = client.newIterator()
  override def close(): Unit = client.close()

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()
    def put(key: Array[Byte], data: Array[Byte]) = {
      batch.put(key,data)
    }

    def remove(key: Array[Byte]) = batch.remove(key)
    def write(): Boolean = {
      val status = Option(client.write(new WriteOptions(), batch)) match {
        case Some(_) => true
        case None => false
      }
      batch.close()
      status
    }
  }
}

private object RocksDbConnection {
  val rocksDBStoragesPath = DB.TransactionDataDirName
}
