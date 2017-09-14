package benchmark.database

import java.io.File

import org.apache.commons.io.FileUtils
import org.rocksdb.{Options, RocksDB, WriteBatch, WriteOptions}
import RocksdDb._

private object RocksdDb {
  val dbName = "producer_transaction_db"

  val dbPath = "/tmp/benchmark/rocksdb"

  val dbOptions: Options =
    new Options()
      .setCreateIfMissing(true)

  val writeOptions: WriteOptions =
    new WriteOptions()
}

class RocksdDb
  extends BatchTimeMeasurable {

  RocksDB.loadLibrary()

  private val rocksDb = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)

    RocksDB.open(
      dbOptions,
      dbPath
    )
  }

  override def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    val batch = new WriteBatch()

    records.foreach { record =>
      batch.put(record._1, record._2)
    }

    scala.util.Try(
      rocksDb.write(
        writeOptions,
        batch
      )
    ).isSuccess
  }
}
