package com.bwsw.tstreamstransactionserver.options

import org.rocksdb.{CompressionType, Options}

object ServerOptions {
  case class BootstrapOptions(host: String = "127.0.0.1", port: Int = 8071, threadPool: Int = 4)

  case class StorageOptions(path: String = "/tmp", clearDelayMs: Int = 10, clearAmount: Int = 200,
                            streamDirectory: String = "stream", consumerDirectory: String = "consumer",
                            dataDirectory: String = "transaction_data", metadataDirectory: String = "transaction_metadata",
                            streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                            metadataStorageName: String = "TransactionStore",
                            openedTransactionsStorageName: String = "TransactionOpenStore",
                            berkeleyReadThreadPool: Int = 2, ttlAddMs: Int = 50)

  case class ServerReplicationOptions(endpoints: String = "127.0.0.1:8071", name: String = "server", group: String = "group")

  case class RocksStorageOptions(writeThreadPool: Int = 4, readThreadPoll: Int = 2, ttlAddMs: Int = 50,
                                 createIfMissing: Boolean = true, maxBackgroundCompactions: Int = 1,
                                 allowOsBuffer: Boolean = true, compression: CompressionType = CompressionType.LZ4_COMPRESSION,
                                 useFsync: Boolean = true) {
    private lazy val options = new Options().setCreateIfMissing(createIfMissing)
      .setMaxBackgroundCompactions(maxBackgroundCompactions)
      .setAllowOsBuffer(allowOsBuffer)
      .setCompressionType(compression)
      .setUseFsync(useFsync)

    def getDBOptions(): Options = {
      options
    }
  }
}
