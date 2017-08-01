package com.bwsw.tstreamstransactionserver.netty.server.storage


import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbDescriptor, RocksDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage._
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}


class MultiNodeRockStorage(storageOpts: StorageOptions,
                           rocksOpts: RocksStorageOptions,
                           readOnly: Boolean = false)
  extends RocksStorage(storageOpts, rocksOpts, readOnly) {
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
