package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.io.Closeable

import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import org.rocksdb._

class RocksDbConnection(storageOptions: StorageOptions, rocksStorageOpts: RocksStorageOptions, name: String, ttl: Int = -1) extends Closeable {
  RocksDB.loadLibrary()

  private val client =  {
    val path = FileUtils.createDirectory(s"${storageOptions.dataDirectory}/$name", storageOptions.path).getAbsolutePath
    TtlDB.open(rocksStorageOpts.getDBOptions(), path, ttl, false)
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