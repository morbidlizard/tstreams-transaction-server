package benchmark.database

import java.io.File

import com.sleepycat.je._
import BerkeleyDb._
import org.apache.commons.io.FileUtils

private object BerkeleyDb
{
  val dbName = "producer_transaction_db"

  val dbPath = "/tmp/benchmark/berkeley"

  val environmentConfig: EnvironmentConfig =
    new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)

  val databaseConfig: DatabaseConfig =
    new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)

  val transactionConfig: TransactionConfig =
    TransactionConfig.DEFAULT
}


class BerkeleyDb
  extends BatchTimeMeasurable {

  private val environment = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)

    new Environment(
      new File(dbPath),
      environmentConfig
    )
  }

  private val db =
    environment.openDatabase(
      null,
      dbName,
      databaseConfig
    )


  override def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    val transaction =
      environment.beginTransaction(
        null,
        transactionConfig
      )

    records.foreach { record =>
      db.put(
        transaction,
        new DatabaseEntry(record._1),
        new DatabaseEntry(record._2)
      )
    }

    val result = scala.util.Try(
      transaction.commit()
    ).isSuccess
    result
  }
}
