package benchmark.database

import java.io.{Closeable, File}
import java.util

import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.collection.mutable
import RocksDb._
import com.google.common.primitives.UnsignedBytes

private object RocksDb {
  val comparator: util.Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator()

  val dbName = "producer_transaction_db"

  val dbPath = "/home/rakhimov_vv/trans/bm/tmp/rocks_db"

  val dbOptions: Options =
    new Options()
      .setCreateIfMissing(true)

  val readOptions =
    new ReadOptions()

  val writeOptions: WriteOptions =
    new WriteOptions()
}

class RocksDb
  extends AllInOneMeasurable {

  RocksDB.loadLibrary()

  private def init() = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)

    RocksDB.open(
      dbOptions,
      dbPath
    )
  }

  private var rocksDb = init()

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

  override def readALLRecords(): Array[(Array[Byte], Array[Byte])] = {
    val iterator =
      rocksDb.newIterator(readOptions)

    val buffer =
      mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]

    iterator.seekToFirst()
    while (iterator.isValid) {
      buffer += ((iterator.key(), iterator.value()))
      iterator.next()
    }
    iterator.close()

    buffer.toArray
  }

  override def readRecords(from: Array[Byte],
                           to: Array[Byte]): Array[(Array[Byte], Array[Byte])] = {

    val iterator =
      rocksDb.newIterator(readOptions)

    val buffer =
      mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]

    iterator.seek(from)
    while (iterator.isValid && comparator.compare(iterator.key(), to) <= 0) {
      buffer += ((iterator.key(), iterator.value()))
      iterator.next()
    }
    iterator.close()

    buffer.toArray
  }

  override def dropAllRecords(): Unit = {
    close()
    rocksDb = init()
  }


  override def close(): Unit = {
    rocksDb.close()

    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    FileUtils.forceMkdir(file)
  }

  override def toString: String = "rocks_db_statistic"
}
