package com.bwsw.tstreamstransactionserver.netty.server.singleNode


import com.bwsw.tstreamstransactionserver.netty.server.{RocksWriter, Notifier, TestRocksWriter}
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}


class TestSingleNodeServer(authenticationOpts: AuthenticationOptions,
                           zookeeperOpts: CommonOptions.ZookeeperOptions,
                           serverOpts: BootstrapOptions,
                           serverRoleOptions: ServerRoleOptions,
                           serverReplicationOpts: ServerReplicationOptions,
                           storageOpts: StorageOptions,
                           rocksStorageOpts: RocksStorageOptions,
                           commitLogOptions: CommitLogOptions,
                           packageTransmissionOpts: TransportOptions,
                           subscribersUpdateOptions: SubscriberUpdateOptions)
  extends SingleNodeServer(
    authenticationOpts,
    zookeeperOpts,
    serverOpts,
    serverRoleOptions,
    serverReplicationOpts,
    storageOpts,
    rocksStorageOpts,
    commitLogOptions,
    packageTransmissionOpts,
    subscribersUpdateOptions) {

  private lazy val producerNotifier =
    new Notifier[ProducerTransaction]
  private lazy val consumerNotifier =
    new Notifier[ConsumerTransaction]

  override protected lazy val rocksWriter: RocksWriter =
    new TestRocksWriter(
      rocksStorage,
      transactionDataService,
      producerNotifier,
      consumerNotifier
    )

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean,
                                               func: => Unit): Long =
    producerNotifier.leaveRequest(onNotificationCompleted, func)

  final def removeNotification(id: Long): Boolean =
    producerNotifier.removeRequest(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean,
                                               func: => Unit): Long =
    consumerNotifier.leaveRequest(onNotificationCompleted, func)

  final def removeConsumerNotification(id: Long): Boolean =
    consumerNotifier.removeRequest(id)

  override def shutdown(): Unit = {
    super.shutdown()
    if (producerNotifier != null) {
      producerNotifier.close()
    }
    if (consumerNotifier != null) {
      producerNotifier.close()
    }
  }
}
