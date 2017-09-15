package benchmark.database

import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions

import scala.collection.mutable.ArrayBuffer

object Main {

  private val syncWritesTestTrialNumber = 10
  private val asyncWritesTestTrialNumber = 10
  private val readTestTrialNumber = 10
  private val readRecordsInIntervalNumber = Seq(
    10000,
    25000,
    50000,
    100000,
    500000,
    1000000
  )


  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException(
        "Path to database folder and " +
          "name of transaction metadata database folder name should be provided."
      )
    else {
      val rocksStorage = new MultiAndSingleNodeRockStorage(
        SingleNodeServerOptions.StorageOptions(
          path = args(0),
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
        new StatisticCollector()

      val databases = Array(new BerkeleyDb(), new RocksDb(), new MySql())

      databases.foreach { database =>
        collector
          .collectSyncWriteStatistics(
            database,
            recordsAsArray,
            syncWritesTestTrialNumber)
        collector
          .collectAsyncWriteStatistics(
            database,
            recordsAsArray,
            asyncWritesTestTrialNumber)
        collector
          .collectReadStatistics(
            database,
            recordsAsArray,
            readRecordsInIntervalNumber,
            readTestTrialNumber)
      }
      databases.foreach(_.close())
    }
  }
}
