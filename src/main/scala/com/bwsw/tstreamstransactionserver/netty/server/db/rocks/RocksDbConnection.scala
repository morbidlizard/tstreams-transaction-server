package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.io.{Closeable, File}

import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.rocksdb._

class RocksDbConnection(storageOptions: StorageOptions, rocksStorageOpts: RocksStorageOptions, name: String, ttl: Int = -1) extends Closeable {
  RocksDB.loadLibrary()

  private val file = new File(s"${storageOptions.path}/${storageOptions.dataDirectory}/$name")
  private val client =  {
    FileUtils.forceMkdir(file)
    TtlDB.open(rocksStorageOpts.createDBOptions(), file.getAbsolutePath, ttl, false)
  }

  def get(key: Array[Byte]) = client.get(key)
  def put(key: Array[Byte], data: Array[Byte]) = client.put(key, data)


  def iterator = client.newIterator()
  override def close(): Unit = client.close()

  final def closeAndDeleteFodler(): Unit = {
    client.close()
    file.delete()
  }

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()
    def put(key: Array[Byte], data: Array[Byte]) = {
      batch.put(key,data)
    }

    def remove(key: Array[Byte]) = batch.remove(key)
    def write(): Boolean = {
      val status = scala.util.Try(client.write(new WriteOptions(), batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) => false
      }
      batch.close()
      status
    }
  }
}