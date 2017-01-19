package netty.server.db.rocks

import java.io.Closeable

import configProperties.ServerConfig
import org.rocksdb._

class RocksDbConnection(config: ServerConfig, name: String, ttl: Int = -1) extends Closeable {
  RocksDB.loadLibrary()

  private val client =  {
    val path = io.FileUtils.createDirectory(s"${config.dbTransactionDataDirName}/$name", config.dbPath).getAbsolutePath
    TtlDB.open(config.options, path, ttl, false)
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