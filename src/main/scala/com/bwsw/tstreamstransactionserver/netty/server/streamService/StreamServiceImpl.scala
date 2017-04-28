package com.bwsw.tstreamstransactionserver.netty.server.streamService

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL, RocksDBPartitionDatabase}
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, HasEnvironment, StreamCache, Time}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

trait StreamServiceImpl extends Authenticable
  with StreamCache {

  val executionContext: ServerExecutionContext
  val rocksMetaServiceDB: RocksDBALL
  val timer: Time

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val streamDatabase: RocksDBPartitionDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.STREAM_STORE_INDEX)

  def closeRocksDBConnectionAndDeleteFolder(stream: Long): Unit

  def removeLastOpenedAndCheckpointedTransactionRecords(stream: Long, batch: Batch): Unit

  private def fillStreamRAMTable(): Long = {
    val streamRecords = ArrayBuffer[StreamRecord]()
    val iterator = streamDatabase.iterator
    iterator.seekToFirst()

    while (iterator.isValid) {
      val streamKey = StreamKey.fromByteArray(iterator.key())
      val streamValue = StreamValue.fromByteArray(iterator.value())
      val streamRecord = StreamRecord(streamKey, streamValue)
      if (!streamRecord.stream.deleted)
        streamRecords += streamRecord
      iterator.next()
    }
    iterator.close()

    streamRecords.groupBy(_.name).foreach { case (name, streams) => streamCache.put(name, streams) }
    streamRecords.lastOption.map(_.id).getOrElse(0L)
  }

  private val idGen = new AtomicLong(fillStreamRAMTable())

  override def getStreamFromOldestToNewest(stream: String): ArrayBuffer[StreamRecord] =
    if (streamCache.containsKey(stream)) streamCache.get(stream)
    else {
      val streamDoesntExistThrowable = new StreamDoesNotExist(stream)
      if (logger.isDebugEnabled()) logger.debug(streamDoesntExistThrowable.getMessage)
      throw streamDoesntExistThrowable
    }


  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): Boolean = {
    val isOkay = if (streamCache.containsKey(stream)) {
      val streams = streamCache.get(stream)
      val mostRecentStreamRecord = streams.last

      if (mostRecentStreamRecord.stream.deleted) {
        val newStreamKey = StreamKey(idGen.getAndIncrement())
        val newStreamValue = StreamValue(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
        streamCache.get(stream) += StreamRecord(newStreamKey, newStreamValue)
        streamDatabase.put(newStreamKey.toByteArray, newStreamValue.toByteArray)
      } else false
    }
    else {
      val newKey = StreamKey(idGen.getAndIncrement())
      val newStream = StreamValue(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
      streamCache.put(stream, ArrayBuffer(StreamRecord(newKey, newStream)))
      streamDatabase.put(newKey.toByteArray, newStream.toByteArray)
    }

    if (isOkay) {
      if (logger.isDebugEnabled()) logger.debug(s"Stream $stream with number of partitons $partitions, with $ttl and $description is saved successfully.")
    } else {
      if (logger.isDebugEnabled()) logger.debug(s"Stream $stream isn't saved.")
    }

    isOkay
  }

  def checkStreamExists(stream: String): Boolean =
    scala.util.Try(getMostRecentStream(stream)).isSuccess

  def getStream(stream: String): com.bwsw.tstreamstransactionserver.rpc.Stream =
    getMostRecentStream(stream).stream

  def delStream(stream: String): Boolean = {
    scala.util.Try(getStreamFromOldestToNewest(stream)) match {
      case scala.util.Success(streamObjects) =>
        val mostRecentStreamRecord = streamObjects.last
        if (mostRecentStreamRecord.stream.deleted) {
          if (logger.isDebugEnabled()) logger.debug(s"Stream $stream has been already removed.")
          false
        } else {
          val batch = rocksMetaServiceDB.newBatch
          mostRecentStreamRecord.stream.deleted = true
          val isOkay = batch.put(HasEnvironment.STREAM_STORE_INDEX, mostRecentStreamRecord.key.toByteArray, mostRecentStreamRecord.stream.toByteArray)
          if (isOkay) {
            //              removeLastOpenedAndCheckpointedTransactionRecords(mostRecentStreamRecord.id, batch)
            closeRocksDBConnectionAndDeleteFolder(mostRecentStreamRecord.id)
            batch.write()
            if (logger.isDebugEnabled()) logger.debug(s"Stream $stream is removed successfully.")
          } else {
            if (logger.isDebugEnabled()) logger.debug(s"Stream $stream isn't removed.")
          }
          isOkay
        }
      case scala.util.Failure(throwable) => false
    }
  }
}