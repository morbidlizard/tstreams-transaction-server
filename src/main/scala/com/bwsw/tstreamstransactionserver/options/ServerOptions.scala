package com.bwsw.tstreamstransactionserver.options

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy._
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy._
import org.rocksdb.{ColumnFamilyOptions, CompressionType, DBOptions, Options}

object ServerOptions {

  /** The options are applied on bootstrap of a server.
    *
    * @param host ipv4 or ipv6 listen address in string representation.
    * @param port a port.
    * @param orderedExecutionPoolSize a number of pool that contains single thread executor to work with transactions.
    */
  case class BootstrapOptions(host: String = "127.0.0.1",
                              port: Int = 8071,
                              orderedExecutionPoolSize: Int = Runtime.getRuntime.availableProcessors()
                             )

  /** The options are used to provide notification service for subscribers.
    *
    * @param subscribersUpdatePeriodMs delay in milliseconds between updates of current subscribers online.
    * @param subscriberMonitoringZkEndpoints The zookeeper server(s) connect to.
    */
  case class SubscriberUpdateOptions(subscribersUpdatePeriodMs: Int = 1000,
                                     subscriberMonitoringZkEndpoints: String = "127.0.0.1:37001"
                                    )

  /** The options are used to validate client requests by a server.
    *
    * @param key                the key to authorize.
    * @param activeTokensNumber the number of active tokens a server can handle over time.
    * @param tokenTTL           the time a token live before expiration.
    */
  case class AuthOptions(key: String = "",
                         activeTokensNumber: Int = 100,
                         tokenTTL: Int = 300
                        )

  /** The options are used to define folders for databases.
    *
    * @param path              the path where folders of Commit log, berkeley environment and rocksdb databases would be placed.
    * @param streamZookeeperDirectory the zooKeeper path for stream entities.
    * @param dataDirectory     the path where rocksdb databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param metadataDirectory the path where a berkeley environment and it's databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param commitLogDirectory the path where commit log files are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param commitLogRocksDirectory the path where rocksdb with persisted commit log files is placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    *
    */
  case class StorageOptions(path: String = "/tmp",
                            streamZookeeperDirectory: String = "/tts/streams",
                            dataDirectory: String = "transaction_data",
                            metadataDirectory: String = "transaction_metadata",
                            commitLogDirectory: String = "commit_log",
                            commitLogRocksDirectory: String = "commit_log_rocks" //,
                            /** streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                              * metadataStorageName: String = "TransactionStore", openedTransactionsStorageName: String = "TransactionOpenStore",
                              * berkeleyReadThreadPool: Int = 2 */
                           )

  /** The options for generating id for a new commit log file.
    *
    * @param counterPathFileIdGen the coordination path for counter for generating and retrieving commit log file id.
    */
  case class ZooKeeperOptions(counterPathFileIdGen: String = "/server_counter/file_id_gen")

  /** The options are used for replication environment.
    *
    * @param authKey the special security token which is used by the slaves to authenticate on master.
    * @param endpoints ???
    * @param name ???
    * @param group ???
    */
  case class ServerReplicationOptions(authKey: String = "server_group",
                                      endpoints: String = "127.0.0.1:8071",
                                      name: String = "server",
                                      group: String = "group"
                                     )

  /** The options are applied on creation Rocksdb database.
    * For all rocksDB options look: https: //github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
    * For performance optimization intuition: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    *
    *
    * @param writeThreadPool          the number of threads of pool are used to do write operations from Rocksdb databases.
    *                                 Used for [[com.bwsw.tstreamstransactionserver.netty.server.ServerHandler]]
    * @param readThreadPool           the number of threads of pool are used to do read operations from Rocksdb databases.
    *                                 Used for [[com.bwsw.tstreamstransactionserver.netty.server.ServerHandler]]
    * @param ttlAddMs                 the time to add to [[com.bwsw.tstreamstransactionserver.rpc.StreamValue.ttl]] that is used to, with stream ttl, to determine how long all producer transactions data belonging to one stream live.
    * @param transactionCacheSize     the max number of producer data units at one point of time LRU cache can contain.
    * @param transactionDatabaseTransactionKeeptimeMin the lifetime of a producer transaction after persistence to database.(default: 6 months). If negative integer - transaction lives forever.
    * @param maxBackgroundCompactions is the maximum number of concurrent background compactions. The default is 1, but to fully utilize your CPU and storage you might want to increase this to approximately number of cores in the system.
    * @param compression Compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION].
    *                    If it's unimportant use a LZ4_COMPRESSION as default value.
    * @param useFsync if true, then every store to stable storage will issue a fsync.
    *                 If false, then every store to stable storage will issue a fdatasync.
    *                 This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
    */
  case class RocksStorageOptions(writeThreadPool: Int = 2,
                                 readThreadPool: Int = 2,
                                 ttlAddMs: Int = 50,
                                 transactionDatabaseTransactionKeeptimeMin: Int = TimeUnit.DAYS.toMillis(180).toInt,
                                 transactionCacheSize: Int = 300,
                                 maxBackgroundCompactions: Int = 1,
                                 compression: CompressionType = CompressionType.LZ4_COMPRESSION,
                                 useFsync: Boolean = true
                                ) {



    def createDBOptions(maxBackgroundCompactions: Int = this.maxBackgroundCompactions,
                        useFsync: Boolean = this.useFsync): DBOptions = {

      new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setUseFsync(useFsync)
    }

    def createOptions(maxBackgroundCompactions: Int = this.maxBackgroundCompactions,
                     compression: CompressionType = this.compression,
                     useFsync: Boolean = this.useFsync): Options = {
      new Options()
        .setCreateIfMissing(true)
        .setCompressionType(compression)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setUseFsync(useFsync)
    }


    def createChilderCollumnOptions(compression: CompressionType = this.compression): ColumnFamilyOptions ={
      new ColumnFamilyOptions()
        .setCompressionType(compression)
    }
  }

  /** The options are applied when client transmit transactions or producer transactions data to server. On setting options also take into consideration [[com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions.requestTimeoutMs]].
    *
    * @param maxMetadataPackageSize the size of metadata package that client can transmit or request to/from server, i.e. calling 'scanTransactions' method.
    *                               If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception.
    *                               If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client.
    *                               If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset.
    *
    * @param maxDataPackageSize the size of data package that client can transmit or request to/from server, i.e. calling 'getTransactionData' method.
    *                           If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception.
    *                           If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client.
    *                           If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset.
    */
  case class TransportOptions(maxMetadataPackageSize: Int = 100000000,
                              maxDataPackageSize: Int = 100000000
                             )

  /** The options are applied when processing commit log files.
    *
    * @param commitLogWriteSyncPolicy policies to work with commitlog.
    *                                 If 'every-n-seconds' mode is chosen then data is flushed into file when specified count of seconds from last flush operation passed.
    *                                 If 'every-new-file' mode is chosen then data is flushed into file when new file starts.
    *                                 If 'every-nth' mode is chosen then data is flushed into file when specified count of write operations passed.
    * @param commitLogWriteSyncValue  count of write operations or count of seconds between flush operations. It depends on the selected policy.
    * @param incompleteCommitLogReadPolicy policies to read from commitlog files.
    *                                      If 'resync-majority' mode is chosen then ???(not implemented yet).
    *                                      If 'skip-log' mode is chosen commit log files than haven't md5 file are not read.
    *                                      If 'try-read' mode is chosen commit log files than haven't md5 file are tried to be read.
    *                                      If 'error' mode is chosen commit log files than haven't md5 file throw throwable and stop server working.
    * @param commitLogCloseDelayMs the time through a commit log file is closed.
    * @param commitLogFileTtlSec the time a commit log files live before they are deleted.
    */
  case class CommitLogOptions(commitLogWriteSyncPolicy: CommitLogWriteSyncPolicy = EveryNewFile,
                              commitLogWriteSyncValue: Int = 0,
                              incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy = SkipLog,
                              commitLogCloseDelayMs: Int = 200,
                              commitLogFileTtlSec: Int = 86400
                             )
}

