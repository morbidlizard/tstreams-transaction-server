package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.exception.Throwable._
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.shared.FNV
import com.bwsw.tstreamstransactionserver.utils.FileUtils
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import transactionService.rpc.StreamService

import scala.concurrent.{Future => ScalaFuture}

trait StreamServiceImpl extends StreamService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

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


  override def getStreamDatabaseObject(stream: String): KeyStream =
    if (streamTTL.containsKey(stream)) streamTTL.get(stream)
    else {
      val key = Key(FNV.hash64a(stream.getBytes()).toLong)

      val keyEntry = key.toDatabaseEntry
      val streamEntry = new DatabaseEntry()

      if (streamDatabase.get(null, keyEntry, streamEntry, LockMode.READ_COMMITTED) == OperationStatus.SUCCESS) {
        logger.debug(s"Stream $stream is retrieved successfully.")
        KeyStream(key, Stream.entryToObject(streamEntry))
      }
      else {
        logger.debug(s"Stream $stream doesn't exist.")
        throw new StreamDoesNotExist
      }
    }


  override def putStream(token: Int, stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Boolean] =
    authenticate(token) {
      val newStream = Stream(stream, partitions, description, ttl)
      val newKey = Key(FNV.hash64a(stream.getBytes()).toLong)
      streamTTL.putIfAbsent(stream, KeyStream(newKey, newStream))

      val transactionDB = streamEnvironment.beginTransaction(null, new TransactionConfig())
      val result = streamDatabase.putNoOverwrite(transactionDB, newKey.toDatabaseEntry, newStream.toDatabaseEntry)
      if (result == OperationStatus.SUCCESS) {
        transactionDB.commit()
        logger.debug(s"Stream $stream is saved successfully.")
        true
      } else {
        transactionDB.abort()
        logger.debug(s"Stream $stream isn't saved.")
        false
      }
    }(executionContext.berkeleyWriteContext)

  override def checkStreamExists(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token)(scala.util.Try(getStreamDatabaseObject(stream).stream).isSuccess)(executionContext.berkeleyReadContext)

  override def getStream(token: Int, stream: String): ScalaFuture[Stream] =
    authenticate(token)(getStreamDatabaseObject(stream).stream)(executionContext.berkeleyReadContext)

  override def delStream(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token) {
      val key = Key(FNV.hash64a(stream.getBytes()).toLong)
      streamTTL.remove(stream)
      val keyEntry = key.toDatabaseEntry
      val transactionDB = streamEnvironment.beginTransaction(null, new TransactionConfig())
      val result = streamDatabase.delete(transactionDB, keyEntry)
      if (result == OperationStatus.SUCCESS) {
        transactionDB.commit()
        logger.debug(s"Stream $stream is removed successfully.")
        true
      } else {
        logger.debug(s"Stream $stream isn't removed.")
        transactionDB.abort()
        false
      }
    }(executionContext.berkeleyWriteContext)


  def closeStreamEnvironmentAndDatabase(): Unit = {
    scala.util.Try(streamDatabase.close())
    scala.util.Try(streamEnvironment.close())
  }
}