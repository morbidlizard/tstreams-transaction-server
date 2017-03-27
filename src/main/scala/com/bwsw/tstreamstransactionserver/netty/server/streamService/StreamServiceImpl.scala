package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionStreamPartition
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache, Time}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.bind.tuple.StringBinding
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import com.bwsw.tstreamstransactionserver.rpc.StreamService

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

    val keyStreams = ArrayBuffer[KeyStream]()
    val cursor = streamDatabase.openCursor(new DiskOrderedCursorConfig())
    while (cursor.getNext(keyFound, dataFound, null) == OperationStatus.SUCCESS) {
      val key = Key.entryToObject(keyFound)
      val streamWithoutKey = StreamWithoutKey.entryToObject(dataFound)
      keyStreams += KeyStream(key, streamWithoutKey)
    }
    cursor.close()

    keyStreams.groupBy(_.name).foreach{case(name, streams) => streamCache.put(name, streams)}
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

  override def getStreamFromOldestToNewest(stream: String): ArrayBuffer[KeyStream] =
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
        val mostRecentKeyStream = streams.last

        if (mostRecentKeyStream.stream.deleted) {
          val newKey = Key(streamSeq.get(transactionDB, 1))
          val newStream = StreamWithoutKey(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
          streamCache.get(stream) += KeyStream(newKey, newStream)
          streamDatabase.putNoOverwrite(transactionDB, newKey.toDatabaseEntry, newStream.toDatabaseEntry) == OperationStatus.SUCCESS
        } else false
      }
      else {
        val newKey = Key(streamSeq.get(transactionDB, 1))
        val newStream = StreamWithoutKey(stream, partitions, description, ttl, timer.getCurrentTime, deleted = false)
        streamCache.put(stream, ArrayBuffer(KeyStream(newKey, newStream)))
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
    ScalaFuture.successful(scala.util.Try(getStreamFromOldestToNewest(stream).nonEmpty).isSuccess)

  override def getStream(stream: String): ScalaFuture[com.bwsw.tstreamstransactionserver.rpc.Stream] =
    ScalaFuture{
      val mostRecentStream = getStreamFromOldestToNewest(stream).last.stream
      if (mostRecentStream.deleted) throw new StreamDoesNotExist(stream) else mostRecentStream
    }(executionContext.berkeleyReadContext)

  override def delStream(stream: String): ScalaFuture[Boolean] =
    ScalaFuture{
      scala.util.Try(getStreamFromOldestToNewest(stream)) match {
        case scala.util.Success(streamObjects) =>
          val transactionDB = environment.beginTransaction(null, new TransactionConfig())

          val mostRecentKeyStream = streamObjects.last
          mostRecentKeyStream.stream.deleted = true

          val result = streamDatabase.put(transactionDB, mostRecentKeyStream.key.toDatabaseEntry, mostRecentKeyStream.stream.toDatabaseEntry)
          if (result == OperationStatus.SUCCESS) {
            removeLastOpenedAndCheckpointedTransactionRecords(mostRecentKeyStream.streamNameToLong, transactionDB)
            transactionDB.commit()
            closeRocksDBConnectionAndDeleteFolder(mostRecentKeyStream.streamNameToLong)
            if (logger.isDebugEnabled()) logger.debug(s"Stream $stream is removed successfully.")
            true
          } else {
            if (logger.isDebugEnabled()) logger.debug(s"Stream $stream isn't removed.")
            transactionDB.abort()
            false
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