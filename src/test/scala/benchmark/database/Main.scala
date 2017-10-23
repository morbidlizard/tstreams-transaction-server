package benchmark.database

import sys.process._

import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions

import scala.collection.mutable.ArrayBuffer

object Main {

  private val syncWritesTestTrialNumber = 2
  private val asyncWritesTestTrialNumber = 2
  private val readTestTrialNumber = 2
  private val readRecordsInIntervalNumber = Seq(
    10000,
    25000,
    50000,
    100000,
    250000,
    500000,
    750000,
    1000000
  )

  private val databaseConstructors = Array[String => AllInOneMeasurable](
    path => new BerkeleyDb(path),
    path => new RocksDb(path),
    // path => new MySql()
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException(
        "Path to database folder and " +
          "name of transaction metadata database folder name should be provided."
      )
    else {
      val path = args(0)

      val databases = databaseConstructors
        .map(_(path))
        .toList

      val rocksStorage = new MultiAndSingleNodeRockStorage(
        SingleNodeServerOptions.StorageOptions(
          path = s"${path}/input",
          metadataDirectory = args(1)
        ),
        SingleNodeServerOptions.RocksStorageOptions(
          transactionExpungeDelayMin = -1
        ),
        readOnly = true
      )
      val database =
        rocksStorage.getStorageManager.getDatabase(Storage.TRANSACTION_ALL_STORE)

      val iterator = database.iterator
      iterator.seekToFirst()

      val records = ArrayBuffer.empty[(Array[Byte], Array[Byte])]
      while (iterator.isValid) {
        val key = iterator.key
        val value = iterator.value()

        records += ((key, value))

        iterator.next()
      }
      iterator.close()

      val recordsAsArray =
        records.toArray

      val collector =
        new StatisticCollector(s"${path}/output")

      databases.foreach { database =>
        /**/
        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            1, // thread number
            0, // shift ratio
            asyncWritesTestTrialNumber)

        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            2, // thread number
            0, // shift ratio
            asyncWritesTestTrialNumber)

        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            2, // thread number
            0.5, // shift ratio
            asyncWritesTestTrialNumber)
        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            4, // thread number
            0, // shift ratio
            asyncWritesTestTrialNumber)
        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            4, // thread number
            0.25, // shift ratio
            asyncWritesTestTrialNumber)
        /*
        collector
          .collectReadStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            readTestTrialNumber)
        */
      }
      databases.foreach(_.close())

      s"python3 src/test/scala/benchmark/database/py/graphs.py $path" !!
    }
  }
}
