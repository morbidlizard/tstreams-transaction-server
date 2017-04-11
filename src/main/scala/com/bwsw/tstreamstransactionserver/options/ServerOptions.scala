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
    * @param path              the path where folders of Commit log, berkeley environment and rocksdb databases would be placed.
    * @param dataDirectory     the path where rocksdb databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param metadataDirectory the path where a berkeley environment and it's databases are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param commitLogDirectory the path where commit log files are placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    * @param commitLogRocksDirectory the path where rocksdb with persisted commit log files is placed relatively to [[com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions.path]]
    *
    */
  case class StorageOptions(path: String = "/tmp",
                            dataDirectory: String = "transaction_data", metadataDirectory: String = "transaction_metadata",
                            commitLogDirectory: String = "commmit_log", commitLogRocksDirectory: String = "commit_log_rocks"//,
                            /** streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                              * metadataStorageName: String = "TransactionStore", openedTransactionsStorageName: String = "TransactionOpenStore",
                              * berkeleyReadThreadPool: Int = 2 */)

  /** The options for berkeley db je.
    *
    * @param berkeleyReadThreadPool the number of threads of pool are used to do read operations from BerkeleyDB je databases.
    *                               Used for [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.scanTransactions]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.getTransaction]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.TransactionMetaServiceImpl.getLastCheckpoitnedTransaction]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl.getStream]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamServiceImpl.checkStreamExists]],
    *                               [[com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl.getConsumerState]]
    *
    */
  case class BerkeleyStorageOptions(berkeleyReadThreadPool: Int = 2) extends AnyVal

  /** The options for generating id for a new commit log file.
    *
    * @param counterPathFileIDGen the coordination path for counter for generating and retrieving commit log file id.
    * @param counterPathFileRecordIDGen the coordination path for counter for generating and retrieving commit log file record id.
    */
  case class ZooKeeperOptions(counterPathFileIDGen: String = "/server_counter/file_id_gen", counterPathFileRecordIDGen: String = "/server_counter/file_record_id_gen")

  /** The options are used for replication environment.
    *
    * @param endpoints ???
    * @param name ???
    * @param group ???
    */
  case class ServerReplicationOptions(endpoints: String = "127.0.0.1:8071", name: String = "server", group: String = "group")

  /** The options are applied on creation Rocksdb database.
    * For all rocksDB options look: https: //github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
    * For performance optimization intuition: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    *
    *
    * @param writeThreadPool the number of threads of pool are used to do write operations from Rocksdb databases.
    *                        Used for [[com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl.putTransactionData]]
    * @param readThreadPool the number of threads of pool are used to do read operations from Rocksdb databases.
    *                       Used for [[com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataServiceImpl.getTransactionData]]
    * @param ttlAddMs the time to add to [[com.bwsw.tstreamstransactionserver.rpc.Stream.ttl]] that is used to, with stream ttl, to determine how long all producer transactions data belonging to one stream live.
    * @param createIfMissing if true, the database will be created if it is missing.
    * @param maxBackgroundCompactions is the maximum number of concurrent background compactions. The default is 1, but to fully utilize your CPU and storage you might want to increase this to approximately number of cores in the system.
    * @param allowOsBuffer if false, we will not buffer files in OS cache. Look at: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    * @param compression Compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION].
    *                    If it's unimportant use a LZ4_COMPRESSION as default value.
    * @param useFsync if true, then every store to stable storage will issue a fsync.
    *                 If false, then every store to stable storage will issue a fdatasync.
    *                 This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
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

      new Options()
        .setCreateIfMissing(createIfMissing)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        //.setAllowOsBuffer(allowOsBuffer)
        .setCompressionType(compression)
        .setUseFsync(useFsync)
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
  case class TransportOptions(maxMetadataPackageSize: Int = 100000000, maxDataPackageSize: Int = 100000000)

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
    * @param commitLogFileTTLSec the time a commit log files live before they are deleted.
    */
  case class CommitLogOptions(commitLogWriteSyncPolicy: CommitLogWriteSyncPolicy = EveryNewFile,
                              commitLogWriteSyncValue: Int = 0,
                              incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy = SkipLog,
                              commitLogCloseDelayMs: Int = 200,
                              commitLogFileTTLSec: Int = 86400
                             )
}

