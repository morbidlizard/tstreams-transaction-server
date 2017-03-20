package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy._
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy._
import org.rocksdb.{CompressionType, Options}

object ServerOptions {

  case class BootstrapOptions(host: String = "127.0.0.1", port: Int = 8071, threadPool: Int = 4)

  case class AuthOptions(key: String = "", activeTokensNumber: Int = 100, tokenTtl: Int = 300)

  case class StorageOptions(path: String = "/tmp", clearDelayMs: Int = 10, clearAmount: Int = 200,
                            streamDirectory: String = "stream", consumerDirectory: String = "consumer",
                            dataDirectory: String = "transaction_data", metadataDirectory: String = "transaction_metadata",
                            streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                            metadataStorageName: String = "TransactionStore",
                            openedTransactionsStorageName: String = "TransactionOpenStore",
                            berkeleyReadThreadPool: Int = 2)

  case class ServerReplicationOptions(endpoints: String = "127.0.0.1:8071", name: String = "server", group: String = "group")

  case class RocksStorageOptions(writeThreadPool: Int = 4, readThreadPool: Int = 2, ttlAddMs: Int = 50,
                                 createIfMissing: Boolean = true, maxBackgroundCompactions: Int = 1,
                                 allowOsBuffer: Boolean = true, compression: CompressionType = CompressionType.LZ4_COMPRESSION,
                                 useFsync: Boolean = true) {

    def createDBOptions(createIfMissing: Boolean = this.createIfMissing,
                        maxBackgroundCompactions: Int = this.maxBackgroundCompactions,
                        allowOsBuffer: Boolean = this.allowOsBuffer,
                        compression: CompressionType = this.compression,
                        useFsync: Boolean = this.useFsync): Options = {

      new Options().setCreateIfMissing(createIfMissing)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setAllowOsBuffer(allowOsBuffer)
        .setCompressionType(compression)
        .setUseFsync(useFsync)
    }
  }

  case class PackageTransmissionOptions(maxMetadataPackageSize: Int = 100000000, maxDataPackageSize: Int = 100000000)

  case class CommitLogOptions(commitLogWriteSyncPolicy: CommitLogWriteSyncPolicy = EveryNewFile,
                              commitLogWriteSyncValue: Int = 0,
                              incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy = SkipLog,
                              maxIdleTimeBetweenRecords: Int = 2)
}

