package benchmark.database

import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 2)
      throw new IllegalArgumentException(
        "Path to database folder and name of transaction metadata database folder name should be provided."
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

//      val databases = ()


      val rocksDb = new MySql()

      val collector =
        new StatisticCollector()

      collector.collectWriteStatistics(rocksDb, records.toArray.take(100), 15)
      collector.collectReadStatistics(rocksDb, records.toArray.take(100), Seq(25000, 50000), 10)

      rocksDb.close()
    }
  }
}
