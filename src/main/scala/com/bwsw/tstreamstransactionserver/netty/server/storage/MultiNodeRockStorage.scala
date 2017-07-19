package com.bwsw.tstreamstransactionserver.netty.server.storage


import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbAll, RocksDbDescriptor}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage._


class MultiNodeRockStorage(storageOpts: StorageOptions,
                           rocksOpts: RocksStorageOptions,
                           readOnly: Boolean = false)
  extends RocksStorage(storageOpts, rocksOpts, readOnly)
{
  private val rocksMetaServiceDB: KeyValueDatabaseManager = new RocksDbAll(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    rocksOpts,
    commonDescriptors :+ RocksDbDescriptor(bookkeeperLogStoreDescriptorInfo, columnFamilyOptions),
    readOnly
  )

  override def getRocksStorage: KeyValueDatabaseManager = {
    rocksMetaServiceDB
  }
}
