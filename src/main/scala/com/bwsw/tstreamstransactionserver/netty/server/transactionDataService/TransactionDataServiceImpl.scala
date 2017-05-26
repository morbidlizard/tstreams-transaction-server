package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.cache.Cacheable
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamCRUD, StreamKey, StreamRecord}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.slf4j.LoggerFactory

class TransactionDataServiceImpl(storageOpts: StorageOptions,
                                 rocksStorageOpts: RocksStorageOptions,
                                 streamCache: StreamCRUD,
                                 cache: Cacheable[KeyDataSeq, Array[Byte]]
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

  private def getStorageOrCreateIt(streamRecord: StreamRecord) = {
    val key = StorageName(streamRecord.key.id.toString)
    rocksDBStorageToStream.computeIfAbsent(key, (t: StorageName) => {
      val calculatedTTL = calculateTTL(streamRecord.ttl)
      if (logger.isDebugEnabled()) logger.debug(s"Creating new database[stream: ${streamRecord.name}, ttl(in hrs): $calculatedTTL] for persisting and reading transactions data.")
      new RocksDbConnection(rocksStorageOpts, s"$pathForData${key.toString}", calculatedTTL)
    })
  }

  @throws[StreamDoesNotExist]
  private def getStorageOrThrowError(streamRecord: StreamRecord) = {
    val key = StorageName(streamRecord.key.id.toString)
    Option(rocksDBStorageToStream.get(key))
      .getOrElse {
        if (logger.isDebugEnabled()) {
          val calculatedTTL = calculateTTL(streamRecord.ttl)
          logger.debug(s"Database[stream: ${streamRecord.name}, ttl(in hrs): $calculatedTTL] doesn't exits!")
        }
        throw new StreamDoesNotExist(streamRecord.name)
      }
  }


  final def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): Boolean = {
    if (data.isEmpty) true
    else {
      val streamRecord = streamCache
        .getStream(StreamKey(streamID))
        .getOrElse(throw new StreamDoesNotExist(streamID.toString))

      val rocksDB = getStorageOrCreateIt(streamRecord)

      val batch = rocksDB.newBatch

      val rangeDataToSave = from until (from + data.length)
      val key = Key(partition, transaction)
      val keysWithDataIDs = rangeDataToSave map (dataID => KeyDataSeq(key, dataID))

      (keysWithDataIDs zip data) foreach { case (keyWithDataID, datum) =>
        val sizeOfSlicedData = datum.limit() - datum.position()
        val bytes = new Array[Byte](sizeOfSlicedData)
        datum.get(bytes)

        val binaryKey = keyWithDataID.toByteArray
        cache.put(keyWithDataID, bytes)
        batch.put(binaryKey, bytes)
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
      .getStream(StreamKey(streamID))
      .getOrElse(throw new StreamDoesNotExist(streamID.toString))
    val rocksDB = getStorageOrThrowError(streamRecord)

    val key = Key(partition, transaction)
    val toSeqId = key.toByteArray(to)

    val iterator = rocksDB.iterator
    val data = {
      (from to to).toStream
        .map { dataID =>
          val currentDataSeq = KeyDataSeq(key, dataID)
          val data = cache.get(currentDataSeq)
            .map(data => java.nio.ByteBuffer.wrap(data))
            .orElse {
              iterator.seek(currentDataSeq.toByteArray)
              if (iterator.isValid && ByteArray.compare(iterator.key(), toSeqId) <= 0) {
                Some(java.nio.ByteBuffer.wrap(iterator.value()))
              } else {
                None
              }
            }
          data
        }
        .takeWhile(remainData => remainData.isDefined)
        .map(dataOpt => dataOpt.get)
    }.toArray

    iterator.close()
    data
  }

  def closeTransactionDataDatabases(): Unit = rocksDBStorageToStream.values().forEach(_.close())
}