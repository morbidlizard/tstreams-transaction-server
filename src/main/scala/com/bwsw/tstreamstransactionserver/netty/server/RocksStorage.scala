package com.bwsw.tstreamstransactionserver.netty.server


import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDBALL, RocksDatabaseDescriptor}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.RocksStorageOptions

class RocksStorage(storageOpts: StorageOptions, rocksOpts: RocksStorageOptions) {
  private val columnFamilyOptions = rocksOpts.createChilderCollumnOptions()
  val rocksMetaServiceDB: RocksDBALL = new RocksDBALL(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    rocksOpts,
    Seq(
      RocksDatabaseDescriptor("LastOpenedTransactionStorage".getBytes(),       columnFamilyOptions),
      RocksDatabaseDescriptor("LastCheckpointedTransactionStorage".getBytes(), columnFamilyOptions),
      RocksDatabaseDescriptor("ConsumerStore".getBytes(),                      columnFamilyOptions),
      RocksDatabaseDescriptor("CommitLogStore".getBytes(),                     columnFamilyOptions),
      RocksDatabaseDescriptor("TransactionAllStore".getBytes(),
        columnFamilyOptions,
        TimeUnit.MINUTES.toSeconds(rocksOpts.transactionDatabaseTransactionKeeptimeMin).toInt
      ),
      RocksDatabaseDescriptor("TransactionOpenStore".getBytes(),               columnFamilyOptions)
    )
  )
}

object RocksStorage {
  val LAST_OPENED_TRANSACTION_STORAGE = 1
  val LAST_CHECKPOINTED_TRANSACTION_STORAGE = 2
  val CONSUMER_STORE = 3
  val COMMIT_LOG_STORE = 4
  val TRANSACTION_ALL_STORE = 5
  val TRANSACTION_OPEN_STORE = 6
}