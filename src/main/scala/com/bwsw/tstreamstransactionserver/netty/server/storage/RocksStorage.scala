package com.bwsw.tstreamstransactionserver.netty.server.storage

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbDescriptor, RocksDbMeta}
import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDbBatch, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage._
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.rocksdb.ColumnFamilyOptions

object RocksStorage {
  private[storage] final val lastOpenedTransactionStorageDescriptorInfo =
    RocksDbMeta("LastOpenedTransactionStorage")
  private[storage] final val lastCheckpointedTransactionStorageDescriptorInfo =
    RocksDbMeta("LastCheckpointedTransactionStorage")
  private[storage] final val consumerStoreDescriptorInfo =
    RocksDbMeta("ConsumerStore")
  private[storage] final val transactionAllStoreDescriptorInfo =
    RocksDbMeta("TransactionAllStore")
  private[storage] final val transactionOpenStoreDescriptorInfo =
    RocksDbMeta("TransactionOpenStore")
  private[storage] final val commitLogStoreDescriptorInfo =
    RocksDbMeta("CommitLogStore")
  private[storage] final val bookkeeperLogStoreDescriptorInfo =
    RocksDbMeta("BookkeeperLogStore")

  val LAST_OPENED_TRANSACTION_STORAGE: Int =
    lastOpenedTransactionStorageDescriptorInfo.id

  val LAST_CHECKPOINTED_TRANSACTION_STORAGE: Int =
    lastCheckpointedTransactionStorageDescriptorInfo.id

  val CONSUMER_STORE: Int =
    consumerStoreDescriptorInfo.id

  val TRANSACTION_ALL_STORE: Int =
    transactionAllStoreDescriptorInfo.id

  val TRANSACTION_OPEN_STORE: Int =
    transactionOpenStoreDescriptorInfo.id

  val COMMIT_LOG_STORE: Int =
    commitLogStoreDescriptorInfo.id

  val BOOKKEEPER_LOG_STORE: Int =
    bookkeeperLogStoreDescriptorInfo.id
}

abstract class RocksStorage(storageOpts: StorageOptions,
                            rocksOpts: RocksStorageOptions,
                            readOnly: Boolean = false) {
  protected val columnFamilyOptions: ColumnFamilyOptions =
    rocksOpts.createColumnFamilyOptions()

  protected val commonDescriptors =
    scala.collection.immutable.Seq(
      RocksDbDescriptor(lastOpenedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(lastCheckpointedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(consumerStoreDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(transactionAllStoreDescriptorInfo, columnFamilyOptions,
        TimeUnit.MINUTES.toSeconds(rocksOpts.transactionExpungeDelayMin).toInt
      ),
      RocksDbDescriptor(transactionOpenStoreDescriptorInfo, columnFamilyOptions)
    )

  def getRocksStorage: KeyValueDbManager

  final def newBatch: KeyValueDbBatch =
    getRocksStorage.newBatch
}
