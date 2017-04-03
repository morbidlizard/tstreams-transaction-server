package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache, Time}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.rpc.StreamService
import com.sleepycat.bind.tuple.StringBinding
import com.sleepycat.je._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait StreamServiceImpl extends StreamService[ScalaFuture]
  with Authenticable
  with StreamCache
{

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)

  val environment: Environment
  val timer: Time

  def closeRocksDBConnectionAndDeleteFolder(stream: Long): Unit
  def removeLastOpenedAndCheckpointedTransactionRecords(stream: Long, transaction: com.sleepycat.je.Transaction): Unit

  private val streamStoreName = "StreamStore"
  private val streamDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = streamStoreName//storageOpts.streamStorageName
    environment.openDatabase(null, storeName, dbConfig)
  }

  private def fillStreamRAMTable(): Unit = {
    val keyFound  = new DatabaseEntry()
    val dataFound = new DatabaseEntry()

    val streamRecords = ArrayBuffer[StreamRecord]()
    val cursor = streamDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      val streamKey = StreamKey.entryToObject(keyFound)
      val streamValue = StreamValue.entryToObject(dataFound)
      streamRecords += StreamRecord(streamKey, streamValue)
    }
    cursor.close()

    streamRecords.groupBy(_.name).foreach{case(name, streams) => streamCache.put(name, streams)}
  }
  fillStreamRAMTable()


  private val streamSequenceName = s"SEQ_$streamStoreName"
  private val streamSequenceDB = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    environment.openDatabase(null, streamSequenceName, dbConfig)
  }

  private val streamSeq = {
    val key = new DatabaseEntry()
    StringBinding.stringToEntry(streamSequenceName, key)
    streamSequenceDB.openSequence(null, key, new SequenceConfig().setAllowCreate(true))
  }

  override def getStreamFromOldestToNewest(stream: String): ArrayBuffer[StreamRecord] =
    if (streamCache.containsKey(stream)) streamCache.get(stream)
    else {
      val streamDoesntExistThrowable = new StreamDoesNotExist(stream)
      if (logger.isDebugEnabled()) logger.debug(streamDoesntExistThrowable.getMessage)
      throw streamDoesntExistThrowable
    }


  override def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Boolean] =
    ScalaFuture {
      val transactionDB = environment.beginTransaction(null, new TransactionConfig())

      val isOkay = if (streamCache.containsKey(stream)) {
        val streams = streamCache.get(stream)
        val mostRecentStreamRecord = streams.last

        if (mostRecentStreamRecord.stream.deleted) {
          val newStreamKey = StreamKey(streamSeq.get(transactionDB, 1))
          val newStreamValue = StreamValue(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
          streamCache.get(stream) += StreamRecord(newStreamKey, newStreamValue)
          streamDatabase.putNoOverwrite(transactionDB, newStreamKey.toDatabaseEntry, newStreamValue.toDatabaseEntry) == OperationStatus.SUCCESS
        } else false
      }
      else {
        val newKey = StreamKey(streamSeq.get(transactionDB, 1))
        val newStream = StreamValue(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
        streamCache.put(stream, ArrayBuffer(StreamRecord(newKey, newStream)))
        streamDatabase.putNoOverwrite(transactionDB, newKey.toDatabaseEntry, newStream.toDatabaseEntry) == OperationStatus.SUCCESS
      }

      if (isOkay) {
        if (logger.isDebugEnabled()) logger.debug(s"Stream $stream with number of partitons $partitions, with $ttl and $description is saved successfully.")
        transactionDB.commit()
        true
      } else {
        if (logger.isDebugEnabled()) logger.debug(s"Stream $stream isn't saved.")
        transactionDB.abort()
        false
      }

    }(executionContext.berkeleyWriteContext)

  override def checkStreamExists(stream: String): ScalaFuture[Boolean] =
    ScalaFuture(scala.util.Try(getMostRecentStream(stream)).isSuccess)(executionContext.berkeleyReadContext)

  override def getStream(stream: String): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.Stream] =
    ScalaFuture(getMostRecentStream(stream).stream)(executionContext.berkeleyReadContext)

  override def delStream(stream: String): ScalaFuture[Boolean] =
    ScalaFuture {
      scala.util.Try(getStreamFromOldestToNewest(stream)) match {
        case scala.util.Success(streamObjects) =>
          val mostRecentStreamRecord = streamObjects.last
          if (mostRecentStreamRecord.stream.deleted) {
            if (logger.isDebugEnabled()) logger.debug(s"Stream $stream has been already removed.")
            false
          } else {

            val berkeleyTransaction = environment.beginTransaction(null, new TransactionConfig())
            mostRecentStreamRecord.stream.deleted = true
            val result = streamDatabase.put(berkeleyTransaction, mostRecentStreamRecord.key.toDatabaseEntry, mostRecentStreamRecord.stream.toDatabaseEntry)
            if (result == OperationStatus.SUCCESS) {
              removeLastOpenedAndCheckpointedTransactionRecords(mostRecentStreamRecord.streamNameToLong, berkeleyTransaction)
              berkeleyTransaction.commit()
              closeRocksDBConnectionAndDeleteFolder(mostRecentStreamRecord.streamNameToLong)
              if (logger.isDebugEnabled()) logger.debug(s"Stream $stream is removed successfully.")
              true
            } else {
              if (logger.isDebugEnabled()) logger.debug(s"Stream $stream isn't removed.")
              berkeleyTransaction.abort()
              false
            }
          }
        case scala.util.Failure(throwable) => false
      }
    }(executionContext.berkeleyWriteContext)

  def closeStreamDatabase(): Unit = {
    scala.util.Try(streamSeq.close())
    scala.util.Try(streamSequenceDB.close())
    scala.util.Try(streamDatabase.close())
  }
}