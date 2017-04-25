package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import org.rocksdb._

class RocksDBPartitionDatabase(client: TtlDB, databaseHandler: ColumnFamilyHandle) {
  RocksDB.loadLibrary()

  def get(key: Array[Byte]): Array[Byte] = client.get(databaseHandler, key)

  @throws[RocksDBException]
  def put(key: Array[Byte], data: Array[Byte]): Unit = client.put(databaseHandler, key, data)

  def getLastRecord: Option[(Array[Byte], Array[Byte])] = {
    val iterator = client.newIterator(databaseHandler)
    iterator.seekToLast()
    val record = if (iterator.isValid) {
      val keyValue = (iterator.key(), iterator.value())
      Some(keyValue)
    }
    else {
      None
    }
    iterator.close()
    record
  }

  def iterator: RocksIterator = client.newIterator()

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()
    def put(key: Array[Byte], data: Array[Byte]): Unit = {
      batch.put(databaseHandler, key,data)
    }

    def remove(key: Array[Byte]): Unit = batch.remove(databaseHandler, key)
    def write(): Boolean = {
      val writeOptions = new WriteOptions()
      val status = scala.util.Try(client.write(writeOptions, batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) =>
          throwable.printStackTrace()
          false
      }
      writeOptions.close()
      batch.close()
      status
    }
  }

}
