package transactionService.impl

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.sleepycat.je._
import com.twitter.util.Future
import transactionService.rpc.{Stream, StreamService}
import StreamServiceImpl._
import com.sleepycat.bind.tuple.TupleBinding
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, Persistent, PrimaryKey}
import transactionService.impl.`implicit`.Implicits._

trait StreamServiceImpl extends StreamService[Future] {

   def putStream(token: String, stream: String, partitions: Int, description: Option[String]): Future[Boolean] = Future {
     val directory = createDirectory()

     val environmentConfig = new EnvironmentConfig()
       .setAllowCreate(true)
       .setTransactional(true)

     val storeConfig = new StoreConfig()
       .setAllowCreate(true)
       .setTransactional(true)

     val environment = new Environment(directory, environmentConfig)
     val entityStore = new EntityStore(environment, "StreamStore", storeConfig)

     val transactionDB = environment.beginTransaction(null, null)
     val pIdx = entityStore.getPrimaryIndex(classOf[String], classOf[StreamServiceImpl.Stream])
     pIdx.put(transactionDB, new StreamServiceImpl.Stream(stream,partitions,description))

     transactionDB.commit()

     entityStore.close()
     environment.close()
     true
   }

   def getStream(token: String, stream: String): Future[Stream] = Future {
     val directory = new File(pathToDatabases)

     val environmentConfig = new EnvironmentConfig()
       .setTransactional(true)

     val storeConfig = new StoreConfig()
       .setTransactional(true)


     val environment = new Environment(directory, environmentConfig)
     val entityStore = new EntityStore(environment, "StreamStore", storeConfig)

     val pIdx = entityStore.getPrimaryIndex(classOf[String], classOf[StreamServiceImpl.Stream])

     pIdx.get(stream)
   }

   def delStream(token: String, stream: String): Future[Boolean] = ???
}

object StreamServiceImpl {
  final val pathToDatabases = "/tmp"

  final val partitionKey = new DatabaseEntry("partitions")
  final val descriptionKey = new DatabaseEntry("description")

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

  @Entity private class Stream extends transactionService.rpc.Stream {
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
