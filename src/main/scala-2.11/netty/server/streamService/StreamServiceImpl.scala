package netty.server.streamService

import com.sleepycat.je._
import scala.concurrent.{Future => ScalaFuture}
import netty.server.{Authenticable, CheckpointTTL}
import netty.server.streamService.StreamServiceImpl._
import transactionService.rpc.StreamService
import exception.Throwables._
import shared.FNV

trait StreamServiceImpl extends StreamService[ScalaFuture]
  with Authenticable
  with CheckpointTTL
{
  override def getStreamDatabaseObject(stream: String): netty.server.streamService.KeyStream =
    if (streamTTL.containsKey(stream)) streamTTL.get(stream)
    else {
      val key = Key(FNV.hash64a(stream.getBytes()).toLong)

      val keyEntry = key.toDatabaseEntry
      val streamEntry = new DatabaseEntry()

      if (database.get(null, keyEntry, streamEntry,LockMode.READ_COMMITTED) == OperationStatus.SUCCESS)
        KeyStream(key, Stream.entryToObject(streamEntry))
      else
        throw new StreamNotExist
    }


  override def putStream(token: Int, stream: String, partitions: Int, description: Option[String], ttl: Int): ScalaFuture[Boolean] =
    authenticate(token) {
      val newStream = Stream(stream, partitions, description, ttl)
      val newKey    = Key(FNV.hash64a(stream.getBytes()).toLong)
      streamTTL.putIfAbsent(stream, KeyStream(newKey, newStream))

      database.putNoOverwrite(null, newKey.toDatabaseEntry, newStream.toDatabaseEntry) == OperationStatus.SUCCESS
    }(netty.Context.berkeleyWritePool.getContext)

  override def doesStreamExist(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token) (scala.util.Try(getStreamDatabaseObject(stream).stream).isSuccess)(netty.Context.berkeleyReadPool.getContext)

  override def getStream(token: Int, stream: String): ScalaFuture[Stream] =
    authenticate(token) (getStreamDatabaseObject(stream).stream)(netty.Context.berkeleyReadPool.getContext)


  override def delStream(token: Int, stream: String): ScalaFuture[Boolean] =
    authenticate(token) {
      val key = Key(FNV.hash64a(stream.getBytes()).toLong)
      streamTTL.remove(stream)
      val keyEntry = key.toDatabaseEntry
      database.delete(null, keyEntry) == OperationStatus.SUCCESS
    }(netty.Context.berkeleyWritePool.getContext)
}

object StreamServiceImpl {

  val environment = {
    val directory = transactionService.io.FileUtils.createDirectory(configProperties.DB.StreamDirName)
    val environmentConfig = new EnvironmentConfig().setAllowCreate(true)
    new Environment(directory, environmentConfig)
  }

  val database = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
    val storeName = configProperties.DB.StreamStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }

  def close(): Unit = {
    database.close()
    environment.close()
  }
}
