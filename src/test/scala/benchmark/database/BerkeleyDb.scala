package benchmark.database

import java.io.{Closeable, File}
import java.util

import com.sleepycat.je._
import BerkeleyDb._
import com.google.common.primitives.UnsignedBytes
import org.apache.commons.io.FileUtils

import scala.collection.mutable

private object BerkeleyDb
{
  val comparator: util.Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator()

  val dbName = "producer_transaction_db"

  val dbPath = "/home/rakhimovvv/trans/bm/tmp/berkeleydb"

  val lockMode: LockMode = LockMode.READ_UNCOMMITTED_ALL

  val environmentConfig: EnvironmentConfig =
    new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)

  val databaseConfig: DatabaseConfig =
    new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)

  val cursorConfig: CursorConfig =
    CursorConfig.READ_UNCOMMITTED

  val transactionConfig: TransactionConfig =
    TransactionConfig.DEFAULT
}


class BerkeleyDb
  extends AllInOneMeasurable {

  private def init(): (Environment, Database) = {
    val environment = {
      val file = new File(dbPath)
      FileUtils.deleteDirectory(file)
      FileUtils.forceMkdir(file)

      new Environment(
        new File(dbPath),
        environmentConfig
      )
    }

    val db =
      environment.openDatabase(
        null,
        dbName,
        databaseConfig
      )
    (environment, db)
  }

  var (environment, db) = init()


  private def newTransaction() = {
    environment.beginTransaction(
      null,
      transactionConfig
    )
  }


  override def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    val transaction =
      newTransaction()

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

  override def readALLRecords(): Array[(Array[Byte], Array[Byte])] = {
    val transaction =
      newTransaction()

    val cursor =
      db.openCursor(
        transaction,
        cursorConfig
      )

    val keyEntry =
      new DatabaseEntry()

    val valueEntry =
      new DatabaseEntry()

    val isFirstKeyExist =
      cursor.getFirst(
        keyEntry,
        valueEntry,
        lockMode
      ) == OperationStatus.SUCCESS

    val buffer = mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]
    if (isFirstKeyExist) {
      buffer += ((keyEntry.getData, valueEntry.getData))
      while (cursor.getNext(keyEntry, valueEntry, lockMode) == OperationStatus.SUCCESS) {
        buffer += ((keyEntry.getData, valueEntry.getData))
      }
    }
    cursor.close()
    transaction.commit()
    buffer.toArray
  }

  override def readRecords(from: Array[Byte],
                           to: Array[Byte]): Array[(Array[Byte], Array[Byte])] = {
    val transaction =
      newTransaction()

    val cursor =
      db.openCursor(
        transaction,
        cursorConfig
      )

    val keyEntry =
      new DatabaseEntry(from)

    val valueEntry =
      new DatabaseEntry()

    val isKeyExist =
      cursor.getSearchKeyRange(
        keyEntry,
        valueEntry,
        lockMode
      ) == OperationStatus.SUCCESS

    val buffer = mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]
    if (isKeyExist && comparator.compare(keyEntry.getData(), to) <= 0) {
      buffer += ((keyEntry.getData, valueEntry.getData))
      while (cursor.getNext(keyEntry, valueEntry, lockMode) == OperationStatus.SUCCESS &&
        comparator.compare(keyEntry.getData(), to) <= 0) {
        buffer += ((keyEntry.getData, valueEntry.getData))
      }
    }

    cursor.close()
    transaction.commit()
    buffer.toArray

  }

  override def dropAllRecords(): Unit = {
    close()
    val result = init()
    environment = result._1
    db = result._2
  }


  override def close(): Unit = {
    db.close()
    environment.close()

    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)
  }

  override def toString: String = "berkeley_db_statistic"
}
