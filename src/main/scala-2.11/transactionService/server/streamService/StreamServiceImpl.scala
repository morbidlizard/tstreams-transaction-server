package transactionService.server.streamService

import com.sleepycat.je._
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.server.streamService.StreamServiceImpl._
import transactionService.rpc.StreamService
import exception.Throwables._

trait StreamServiceImpl extends StreamService[TwitterFuture]
  with Authenticable
  with CheckpointTTL
{

  def getStreamTTL(stream: String): TwitterFuture[Int] =
    if (streamTTL.containsKey(stream)) TwitterFuture.value(streamTTL.get(stream))
    else {
      TwitterFuture(pIdx.get(stream)) flatMap { streamObj => if (streamObj != null) {
        streamTTL.put(streamObj.name, streamObj.ttl)
        TwitterFuture.value(streamObj.ttl)
      } else TwitterFuture.exception(throw new StreamNotExist)
      }
  }

  def putStream(token: String, stream: String, partitions: Int, description: Option[String], ttl: Int): TwitterFuture[Boolean] =
    authenticate(token) {
      pIdx.put(new Stream(stream, partitions, description, ttl))
      streamTTL.putIfAbsent(stream, ttl)
      true
    }

  def doesStreamExist(token: String, stream: String): TwitterFuture[Boolean] =
    authenticate(token) (if (pIdx.get(stream) == null) false else true)

  def getStream(token: String, stream: String): TwitterFuture[Stream] =
    authenticate(token) (pIdx.get(stream))


  def delStream(token: String, stream: String): TwitterFuture[Boolean] =
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
