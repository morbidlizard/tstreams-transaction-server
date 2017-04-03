package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamRecord
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.rpc.TransactionDataService
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait TransactionDataServiceImpl extends TransactionDataService[ScalaFuture]
  with Authenticable
  with StreamCache {

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions
  val rocksStorageOpts: RocksStorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val ttlToAdd: Int = rocksStorageOpts.ttlAddMs

  private def calculateTTL(ttl: Long): Int = {
    val convertedTTL = TimeUnit.SECONDS.toHours(ttl + ttlToAdd)
    if (convertedTTL == 0L) scala.math.abs(TimeUnit.HOURS.toSeconds(1L).toInt)
    else scala.math.abs(TimeUnit.HOURS.toSeconds(convertedTTL).toInt)
  }

  private val rocksDBStorageToStream = new java.util.concurrent.ConcurrentHashMap[StorageName, RocksDbConnection]()

  final def removeRocksDBDatabaseAndDeleteFolder(stream: Long): Unit = {
    val key = StorageName(stream.toString)
    Option(rocksDBStorageToStream.get(key)) foreach (x => x.closeAndDeleteFodler())
  }

  private def getStorage(keyStream: StreamRecord, ttl: Long) = {
    val key = StorageName(keyStream.key.streamNameAsLong.toString)
    rocksDBStorageToStream.computeIfAbsent(key, (t: StorageName) => {
      val calculatedTTL = calculateTTL(ttl)
      if (logger.isDebugEnabled()) logger.debug(s"Creating new database[stream: ${keyStream.name}, ttl(in hrs): $calculatedTTL] for persisting and reading transactions data.")
      new RocksDbConnection(storageOpts, rocksStorageOpts, key.toString, calculatedTTL)
    })
  }

  override def putTransactionData(stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): ScalaFuture[Boolean] = {
    val streamObj = getStreamFromOldestToNewest(stream).last
    val rocksDB = getStorage(streamObj, streamObj.stream.ttl)
    ScalaFuture {
      val batch = rocksDB.newBatch

      val rangeDataToSave = from until (from + data.length)
      val keys = rangeDataToSave map (seqId => KeyDataSeq(Key(partition, transaction), seqId).toBinary)

      (keys zip data) foreach { case (key, datum) =>
        val sizeOfSlicedData = datum.limit() - datum.position()
        val bytes = new Array[Byte](sizeOfSlicedData)
        datum.get(bytes)
        batch.put(key, bytes)
      }
      val isOkay = batch.write()

      if (isOkay && logger.isDebugEnabled)
        logger.debug(s"On stream $stream, partition: $partition, transaction $transaction saved transaction data successfully.")
      else
        logger.debug(s"On stream $stream, partition: $partition, transaction $transaction transaction data wasn't saved.")

      isOkay
    }(executionContext.rocksWriteContext)
  }


  override def getTransactionData(stream: String, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[ByteBuffer]] = {
    val streamObj = getStreamFromOldestToNewest(stream).last
    val rocksDB = getStorage(streamObj, streamObj.stream.ttl)
    ScalaFuture {
      val fromSeqId = KeyDataSeq(Key(partition, transaction), from).toBinary
      val toSeqId = KeyDataSeq(Key(partition, transaction), to).toBinary

      val iterator = rocksDB.iterator
      iterator.seek(fromSeqId)

      val data = new ArrayBuffer[ByteBuffer](to - from)
      while (iterator.isValid && ByteArray.compare(iterator.key(), toSeqId) <= 0) {
        data += java.nio.ByteBuffer.wrap(iterator.value())
        iterator.next()
      }
      iterator.close()
      data
    }(executionContext.rocksReadContext)
  }

  def closeTransactionDataDatabases() = rocksDBStorageToStream.values().forEach(_.close())
}