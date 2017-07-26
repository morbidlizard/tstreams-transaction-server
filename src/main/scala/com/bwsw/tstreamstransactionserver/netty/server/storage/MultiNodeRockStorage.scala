package com.bwsw.tstreamstransactionserver.netty.server.storage


import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbManager, RocksDbDescriptor}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage._


class MultiNodeRockStorage(storageOpts: StorageOptions,
                           rocksOpts: RocksStorageOptions,
                           readOnly: Boolean = false)
  extends RocksStorage(storageOpts, rocksOpts, readOnly)
{
  private val rocksMetaServiceDB: KeyValueDbManager = new RocksDbManager(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    rocksOpts,
    commonDescriptors :+ RocksDbDescriptor(bookkeeperLogStoreDescriptorInfo, columnFamilyOptions),
    readOnly
  )

  override def getRocksStorage: KeyValueDbManager = {
    rocksMetaServiceDB
  }
}
