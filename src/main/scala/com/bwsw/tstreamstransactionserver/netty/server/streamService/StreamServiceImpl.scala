package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.sleepycat.bind.tuple.StringBinding
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import transactionService.rpc.StreamService

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future => ScalaFuture}

trait StreamServiceImpl extends StreamService[ScalaFuture]
  with Authenticable
  with StreamCache {

  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)

  val streamEnvironment = {
    val directory = FileUtils.createDirectory(storageOpts.streamDirectory, storageOpts.path)
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setSharedCache(true)
      .setTransactional(true)
    new Environment(directory, environmentConfig)
  }

  val streamDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = storageOpts.streamStorageName
    streamEnvironment.openDatabase(null, storeName, dbConfig)
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


  private val streamSequenceName = s"SEQ_${storageOpts.streamStorageName}"
  private val streamSequenceDB = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    streamEnvironment.openDatabase(null, streamSequenceName, dbConfig)
  }

  private val streamSeq = {
    val key = new DatabaseEntry()
    StringBinding.stringToEntry(streamSequenceName, key)
    streamSequenceDB.openSequence(null, key, new SequenceConfig().setAllowCreate(true))
  }

  override def getStreamFromOldestToNewest(stream: String): ArrayBuffer[KeyStream] =
    if (streamCache.containsKey(stream)) streamCache.get(stream)
    else {
      logger.debug(s"StreamWithoutKey $stream doesn't exist.")
      throw new StreamDoesNotExist
    }


  override def putStream(token: Int, stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Boolean] =
    authenticate(token) {
      val transactionDB = streamEnvironment.beginTransaction(null, new TransactionConfig())

      val newStream = StreamWithoutKey(stream, partitions, description, ttl, System.currentTimeMillis(), deleted = false)
      val newKey    = Key(streamSeq.get(transactionDB, 1))

      val result = streamDatabase.putNoOverwrite(transactionDB,  newKey.toDatabaseEntry, newStream.toDatabaseEntry)
      if (result == OperationStatus.SUCCESS) {
        transactionDB.commit()

        if (streamCache.containsKey(stream))
           streamCache.get(stream) += KeyStream(newKey, newStream)
        else
          streamCache.put(stream, ArrayBuffer(KeyStream(newKey, newStream)))

        logger.debug(s"StreamWithoutKey $stream is saved successfully.")
        true
      } else {
        transactionDB.abort()
        logger.debug(s"StreamWithoutKey $stream isn't saved.")
        false
      }
    }(executionContext.berkeleyWriteContext)

  override def checkStreamExists(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token)(scala.util.Try(getStreamFromOldestToNewest(stream).nonEmpty).isSuccess)(executionContext.berkeleyReadContext)

  override def getStream(token: Int, stream: String): ScalaFuture[transactionService.rpc.Stream] =
    authenticate(token){
      val mostRecentStream = getStreamFromOldestToNewest(stream).last.stream
      if (mostRecentStream.deleted) throw new StreamDoesNotExist else mostRecentStream
    }(executionContext.berkeleyReadContext)

  override def delStream(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token) {
      scala.util.Try(getStreamFromOldestToNewest(stream)) match {
        case scala.util.Success(streamObjects) =>
          val transactionDB = streamEnvironment.beginTransaction(null, new TransactionConfig())

          val mostRecentKeyStream = streamObjects.last
          mostRecentKeyStream.stream.deleted = true

          val result = streamDatabase.put(transactionDB, mostRecentKeyStream.key.toDatabaseEntry, mostRecentKeyStream.stream.toDatabaseEntry)
          if (result == OperationStatus.SUCCESS) {
            transactionDB.commit()
            streamCache.remove(stream)
            logger.debug(s"StreamWithoutKey $stream is removed successfully.")
            true
          } else {
            logger.debug(s"StreamWithoutKey $stream isn't removed.")
            transactionDB.abort()
            false
          }
        case scala.util.Failure(throwable) => false
      }
    }(executionContext.berkeleyWriteContext)

  def closeStreamEnvironmentAndDatabase(): Unit = {
    scala.util.Try(streamSeq.close())
    scala.util.Try(streamSequenceDB.close())
    scala.util.Try(streamDatabase.close())
    scala.util.Try(streamEnvironment.close())
  }
}