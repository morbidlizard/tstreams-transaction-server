package transactionService.server.streamService

import com.sleepycat.je._
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.server.streamService.StreamServiceImpl._
import transactionService.rpc.StreamService
import exception.Throwables._
import shared.FNV

trait StreamServiceImpl extends StreamService[TwitterFuture]
  with Authenticable
  with CheckpointTTL
{

  override def getStreamDatabaseObject(stream: String): transactionService.server.streamService.Stream =
    if (streamTTL.containsKey(stream)) streamTTL.get(stream)
    else {
      val streamObj = pIdx.get(stream)
      if (streamObj != null) {
        streamTTL.put(streamObj.name, streamObj)
        streamObj
      } else throw new StreamNotExist
    }

  override def putStream(token: String, stream: String, partitions: Int, description: Option[String], ttl: Int): TwitterFuture[Boolean] =
    authenticate(token) {
      val newStream = new Stream(stream, partitions, description, ttl, FNV.hash64a(stream.getBytes()).toLong)
      streamTTL.putIfAbsent(stream, newStream)
      pIdx.putNoOverwrite(newStream)
    }

  override def doesStreamExist(token: String, stream: String): TwitterFuture[Boolean] =
    authenticate(token) (if (pIdx.get(stream) == null) false else true)

  override def getStream(token: String, stream: String): TwitterFuture[Stream] =
    authenticate(token) (getStreamDatabaseObject(stream))


  override def delStream(token: String, stream: String): TwitterFuture[Boolean] =
    authenticate(token) {
      streamTTL.remove(stream)
      pIdx.delete(stream)
    }
}

object StreamServiceImpl {
  val storeName = configProperties.DB.StreamStoreName

  val directory = transactionService.io.FileUtils.createDirectory(configProperties.DB.StreamDirName)
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
  val environment = new Environment(directory, environmentConfig)
  val entityStore = new EntityStore(environment, storeName, storeConfig)
  val pIdx = entityStore.getPrimaryIndex(classOf[String], classOf[Stream])

  def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}
