package transactionService.server.streamService

import java.io.{Closeable, File}

import com.sleepycat.je._
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.Authenticable
import transactionService.server.streamService.StreamServiceImpl._
import transactionService.rpc.StreamService
import transactionService.exception.Throwables._

trait StreamServiceImpl extends StreamService[TwitterFuture]
  with Closeable
  with Authenticable
{

  def putStream(token: String, stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        pIdx.put(new Stream(stream, partitions, description))
        true
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }

  def isStreamExist(token: String, stream: String): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        if (pIdx.get(stream) == null) false else true
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }

  def getStream(token: String, stream: String): TwitterFuture[Stream] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) TwitterFuture(pIdx.get(stream)) else TwitterFuture.exception(tokenInvalidException)
  }
  def delStream(token: String, stream: String): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) TwitterFuture(pIdx.delete(stream)) else TwitterFuture.exception(tokenInvalidException)
  }

  override def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}

private object StreamServiceImpl {
  final val pathToDatabases = "/tmp"
  final val storeName = "StreamStore"

  val directory = transactionService.io.FileUtils.createDirectory("stream")
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
  val environment = new Environment(directory, environmentConfig)
  val entityStore = new EntityStore(environment, storeName, storeConfig)
  val pIdx = entityStore.getPrimaryIndex(classOf[String], classOf[Stream])
}
