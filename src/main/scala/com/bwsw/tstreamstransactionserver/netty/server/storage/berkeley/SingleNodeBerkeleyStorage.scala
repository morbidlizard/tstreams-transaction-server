package com.bwsw.tstreamstransactionserver.netty.server.storage.berkeley

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDbBatch, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.db.berkeley.BerkeleyDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions

final class SingleNodeBerkeleyStorage(storageOpts: StorageOptions)
  extends Storage {

  private val berkeleyDbMetaService: KeyValueDbManager = new BerkeleyDbManager(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    Array(
      Storage.lastOpenedTransactionStorageDescriptorInfo,
      Storage.lastCheckpointedTransactionStorageDescriptorInfo,
      Storage.consumerStoreDescriptorInfo,
      Storage.transactionAllStoreDescriptorInfo,
      Storage.transactionOpenStoreDescriptorInfo,
      Storage.commitLogStoreDescriptorInfo
    )
  )

  override def getStorageManager: KeyValueDbManager = {
    berkeleyDbMetaService
  }
}
