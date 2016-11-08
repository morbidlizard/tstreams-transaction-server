package transactionService.impl

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import com.sleepycat.je._
import com.twitter.util.Future
import transactionService.rpc.{Stream, StreamService}
import StreamServiceImpl._
import com.sleepycat.bind.tuple.TupleBinding
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import transactionService.impl.`implicit`.Implicits._

trait StreamServiceImpl extends StreamService[Future] {

   def putStream(token: String, stream: String, partitions: Int, description: Option[String]): Future[Boolean] = Future {
     val directory = createDirectory(stream)

     val dbConfig = new DatabaseConfig()
     dbConfig.setAllowCreate(true)
     dbConfig.setTransactional(true)

     val environmentConfig = new EnvironmentConfig()
       .setAllowCreate(true)
       .setTransactional(true)

     val environment = new Environment(directory, environmentConfig)
     val transactionDB = environment.beginTransaction(null, null)
     val database = environment.openDatabase(transactionDB, stream, dbConfig)

     // put to the database via BASE API
     val partitionBinding = TupleBinding.getPrimitiveBinding(classOf[Integer])
     val partitionsData = new DatabaseEntry()
     partitionBinding.objectToEntry(partitions, partitionsData)
     database.put(transactionDB, partitionKey, partitionsData)

     // put to the database via BASE API
     description foreach { str =>
       val descriptionBinding = TupleBinding.getPrimitiveBinding(classOf[String])
       val descriptionData = new DatabaseEntry()
       descriptionBinding.objectToEntry(str, descriptionData)
       database.put(transactionDB, descriptionKey, descriptionData, Put.NO_OVERWRITE,new WriteOptions().setTTL(1,TimeUnit.HOURS))
     }

     transactionDB.commit()

     directory.deleteOnExit()
     database.close()
     environment.close()
     true
   }

   def getStream(token: String, stream: String): Future[Stream] = Future {
     val environmentConfig = new EnvironmentConfig()

     val directory = new File(s"$pathToDatabases/$stream")
     val environment = new Environment(directory, environmentConfig)

     val database = environment.openDatabase(null, stream, null)


     val partitionsData = new DatabaseEntry()
     val partitionBinding = TupleBinding.getPrimitiveBinding(classOf[Integer])
     database.get(null, partitionKey, partitionsData, LockMode.DEFAULT)
     val partitionsRetrieved = partitionBinding.entryToObject(partitionsData)

     val descriptionData = new DatabaseEntry()
     val descriptionBinding = TupleBinding.getPrimitiveBinding(classOf[String])

     val descriptionRetrieved = if (database.get(null, descriptionKey, descriptionData, LockMode.DEFAULT) == OperationStatus.SUCCESS)
       Some(descriptionBinding.entryToObject(descriptionData))
     else
       None


     new Stream {
       override def partitions: Int = partitionsRetrieved
       override def description: Option[String] = descriptionRetrieved
     }
   }

   def delStream(token: String, stream: String): Future[Boolean] = ???
}

object StreamServiceImpl {
  final val pathToDatabases = "/tmp"

  final val partitionKey = new DatabaseEntry("partitions")
  final val descriptionKey = new DatabaseEntry("description")

  def createDirectory(name: String, deleteAtExit: Boolean = true): File = {
    val path = {
      val dir = Paths.get(s"$pathToDatabases/$name")
      if (Files.exists(dir)) dir else java.nio.file.Files.createDirectory(Paths.get(s"$pathToDatabases/$name"))
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
    private var descriptionDB: Option[String] = None

    override def partitions: Int = partitionsDB
    override def description: Option[String] = descriptionDB

    def this(name: String, partitions:Int, description: Option[String]) = {
      this()
      nameDB = name
      partitionsDB = partitions
      descriptionDB = description
    }
  }
}
