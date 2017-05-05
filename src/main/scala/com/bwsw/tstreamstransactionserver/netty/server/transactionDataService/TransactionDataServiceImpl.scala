package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCache, StreamRecord}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class TransactionDataServiceImpl(storageOpts: StorageOptions,
                                 rocksStorageOpts: RocksStorageOptions,
                                 streamCache: StreamCache
                                )
{

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val ttlToAdd: Int = rocksStorageOpts.ttlAddMs

  private def calculateTTL(ttl: Long): Int = scala.math.abs((ttl + ttlToAdd).toInt)

  private val rocksDBStorageToStream = new java.util.concurrent.ConcurrentHashMap[StorageName, RocksDbConnection]()

  final def removeRocksDBDatabaseAndDeleteFolder(stream: Long): Unit = {
    val key = StorageName(stream.toString)
    Option(rocksDBStorageToStream.get(key)) foreach (x => x.closeAndDeleteFolder())
  }

  private val pathForData = s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.dataDirectory}${java.io.File.separatorChar}"

  private def getStorage(streamRecord: StreamRecord) = {
    val key = StorageName(streamRecord.key.id.toString)
    rocksDBStorageToStream.computeIfAbsent(key, (t: StorageName) => {
      val calculatedTTL = calculateTTL(streamRecord.ttl)
      if (logger.isDebugEnabled()) logger.debug(s"Creating new database[stream: ${streamRecord.name}, ttl(in hrs): $calculatedTTL] for persisting and reading transactions data.")
      new RocksDbConnection(rocksStorageOpts, s"$pathForData${key.toString}", calculatedTTL)
    })
  }


  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean = {
    if (data.isEmpty) true
    else {
      val streamRecord = streamCache
        .getStream(streamID)
        .getOrElse(throw new StreamDoesNotExist(streamID.toString))

      val rocksDB = getStorage(streamRecord)

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

      if (logger.isDebugEnabled) {
        if (isOkay)
          logger.debug(s"On stream ${streamRecord.id}, partition: $partition, transaction $transaction saved transaction data successfully.")
        else
          logger.debug(s"On stream ${streamRecord.id}, partition: $partition, transaction $transaction transaction data wasn't saved.")
      }

      isOkay
    }
  }

  def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): Seq[ByteBuffer] = {
    val streamRecord = streamCache
      .getStream(streamID)
      .getOrElse(throw new StreamDoesNotExist(streamID.toString))
    val rocksDB = getStorage(streamRecord)

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
  }

  def closeTransactionDataDatabases(): Unit = rocksDBStorageToStream.values().forEach(_.close())
}