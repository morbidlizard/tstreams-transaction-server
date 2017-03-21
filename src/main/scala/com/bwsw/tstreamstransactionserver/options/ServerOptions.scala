package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy._
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy._
import org.rocksdb.{CompressionType, Options}

object ServerOptions {

  /** The options are applied on bootstrap of a server.
    *
    * @param host       ipv4 or ipv6 listen address in string representation.
    * @param port       a port.
    * @param threadPool the number of threads of thread pool to serialize/deserialize requests/responses.
    */
  case class BootstrapOptions(host: String = "127.0.0.1", port: Int = 8071, threadPool: Int = 4)

  /** The options are used to validate client requests by a server.
    *
    * @param key                the key to authorize.
    * @param activeTokensNumber the number of active tokens a server can handle over time.
    * @param tokenTTL           the time a token live before expiration.
    */
  case class AuthOptions(key: String = "", activeTokensNumber: Int = 100, tokenTTL: Int = 300)

  /** The options are used to define folders for databases.
    *
    * @param path              the path where there are folders of Commit log, berkeley environment and rocksdb databases.
    * @param dataDirectory     the path where rocksdb databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param metadataDirectory the path where a berkeley environment and it's databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    */
  case class StorageOptions(path: String = "/tmp",
                            dataDirectory: String = "transaction_data", metadataDirectory: String = "transaction_metadata" //,
                            /** streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                              * metadataStorageName: String = "TransactionStore", openedTransactionsStorageName: String = "TransactionOpenStore",
                              * berkeleyReadThreadPool: Int = 2 */)

  /** The options for berkeley db je.
    *
    * @param berkeleyReadThreadPool the number of threads of pool are used to do read operations from berkeley databases.
    *                               Used for [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.scanTransactions]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.getTransaction]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.getLastCheckpoitnedTransaction]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl.getStream]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl.getConsumerState]]
    *
    */
  case class BerkeleyStorageOptions(berkeleyReadThreadPool: Int = 2) extends AnyVal

  /** The options are used for replication environment.
    *
    * @param endpoints ???
    * @param name ???
    * @param group ???
    */
  case class ServerReplicationOptions(endpoints: String = "127.0.0.1:8071", name: String = "server", group: String = "group")

  /**
    *
    * @param writeThreadPool
    * @param readThreadPool
    * @param ttlAddMs
    * @param createIfMissing
    * @param maxBackgroundCompactions
    * @param allowOsBuffer
    * @param compression
    * @param useFsync
    */
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
                              maxIdleTimeBetweenRecords: Int = 2,
                              commitLogToBerkeleyDBTaskDelayMs: Int = 500
                             )

}

