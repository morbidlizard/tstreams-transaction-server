package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.util.concurrent.atomic.AtomicLong

import org.rocksdb.{ColumnFamilyHandle, TtlDB, WriteBatch, WriteOptions}

class Batch(client: TtlDB, databaseHandlers: Seq[ColumnFamilyHandle], idGenerator: AtomicLong) {
  val id: Long = idGenerator.getAndIncrement()

  private val batch  = new WriteBatch()
  def put(index: Int, key: Array[Byte], data: Array[Byte]): Boolean = {
    batch.put(databaseHandlers(index), key, data)
    true
  }

  def remove(index: Int, key: Array[Byte]): Unit = batch.remove(databaseHandlers(index), key)
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

