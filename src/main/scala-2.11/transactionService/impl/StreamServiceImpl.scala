package transactionService.impl

import java.io.{Closeable, File}
import java.nio.file.{Files, Paths}

import com.sleepycat.je._
import com.twitter.util.{Future => TwitterFuture}
import transactionService.rpc.{Stream, StreamService}
import StreamServiceImpl._
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import transactionService.impl.`implicit`.Implicits._

trait StreamServiceImpl extends StreamService[TwitterFuture] with Closeable {
  def putStream(token: String, stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = TwitterFuture {
    pIdx.put(new StreamServiceImpl.Stream(stream, partitions, description))
    true
  }

  def isStreamExist(token: String, stream: String): TwitterFuture[Boolean] = TwitterFuture{
    if (pIdx.get(stream) == null) false else true
  }

  def getStream(token: String, stream: String): TwitterFuture[Stream] = TwitterFuture(pIdx.get(stream))
  def delStream(token: String, stream: String): TwitterFuture[Boolean] = TwitterFuture(pIdx.delete(stream))

  override def close(): Unit = {
    entityStore.close()
    environment.close()
  }
}

private object StreamServiceImpl {
  final val pathToDatabases = "/tmp"
  final val storeName = "StreamStore"

  final val partitionKey = new DatabaseEntry("partitions")
  final val descriptionKey = new DatabaseEntry("description")


  val directory = createDirectory("stream")
  val environmentConfig = new EnvironmentConfig()
    .setAllowCreate(true)
  val storeConfig = new StoreConfig()
    .setAllowCreate(true)
  val environment = new Environment(directory, environmentConfig)
  val entityStore = new EntityStore(environment, storeName, storeConfig)
  val pIdx = entityStore.getPrimaryIndex(classOf[String], classOf[Stream])

  def createDirectory(name: String = pathToDatabases, deleteAtExit: Boolean = true): File = {
    val path = {
      val dir = Paths.get(name)
      if (Files.exists(dir)) dir else java.nio.file.Files.createDirectory(Paths.get(name))
    }

    import org.apache.commons.io.FileUtils

    if (deleteAtExit)
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run() {
          FileUtils.forceDelete(path.toFile)
        }
      })
    path.toFile
  }

  @Entity class Stream extends transactionService.rpc.Stream {
    @PrimaryKey private var nameDB: String = _
    private var partitionsDB: Int = _
    private var descriptionDB:  String = _

    override def partitions: Int = partitionsDB
    override def description: Option[String] = Option(descriptionDB)

    def this(name: String, partitions:Int, description: Option[String]) = {
      this()
      nameDB = name
      partitionsDB = partitions
      description foreach (str => descriptionDB = str)
    }
  }
}
