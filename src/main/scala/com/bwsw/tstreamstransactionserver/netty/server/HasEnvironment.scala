package com.bwsw.tstreamstransactionserver.netty.server


import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDBALL, RocksDatabaseDescriptor}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import org.rocksdb.ColumnFamilyOptions

trait HasEnvironment {
  val storageOpts: StorageOptions

  val rocksMetaServiceDB: RocksDBALL = new RocksDBALL(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    Seq(
      RocksDatabaseDescriptor("StreamStore".getBytes(),                        new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("LastOpenedTransactionStorage".getBytes(),       new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("LastCheckpointedTransactionStorage".getBytes(), new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("ConsumerStore".getBytes(),                      new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("CommitLogStore".getBytes(),                     new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("TransactionAllStore".getBytes(),                new ColumnFamilyOptions()),
      RocksDatabaseDescriptor("TransactionOpenStore".getBytes(),               new ColumnFamilyOptions())
    )
  )
}

object HasEnvironment {
  val STREAM_STORE_INDEX = 1
  val LAST_OPENED_TRANSACTION_STORAGE = 2
  val LAST_CHECKPOINTED_TRANSACTION_STORAGE = 3
  val CONSUMER_STORE = 4
  val COMMIT_LOG_STORE = 5
  val TRANSACTION_ALL_STORE = 6
  val TRANSACTION_OPEN_STORE = 7
}