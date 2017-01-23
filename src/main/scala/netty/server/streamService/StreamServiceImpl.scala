package netty.server.streamService

import com.sleepycat.je._
import configProperties.ServerConfig

import scala.concurrent.{Future => ScalaFuture}
import netty.server.{Authenticable, CheckpointTTL}
import transactionService.rpc.StreamService
import exception.Throwables._
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import shared.FNV

trait StreamServiceImpl extends StreamService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  val config: ServerConfig

  PropertyConfigurator.configure("src/main/resources/logServer.properties")
  private val logger = LoggerFactory.getLogger(classOf[netty.server.Server])

  val streamEnvironment = {
    val directory = io.FileUtils.createDirectory(config.dbStreamDirName, config.dbPath)
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
    val storeName = config.streamStoreName
    streamEnvironment.openDatabase(null, storeName, dbConfig)
  }


  override def getStreamDatabaseObject(stream: String): netty.server.streamService.KeyStream =
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
        throw new StreamNotExist
      }
    }


  override def putStream(token: Int, stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] =
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
    }(config.berkeleyWritePool.getContext)

  override def doesStreamExist(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token)(scala.util.Try(getStreamDatabaseObject(stream).stream).isSuccess)(config.berkeleyReadPool.getContext)

  override def getStream(token: Int, stream: String): ScalaFuture[Stream] =
    authenticate(token)(getStreamDatabaseObject(stream).stream)(config.berkeleyReadPool.getContext)

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
    }(config.berkeleyWritePool.getContext)


  def closeStreamEnviromentAndDatabase(): Unit = {
    Option(streamDatabase.close())
    Option(streamEnvironment.close())
  }
}