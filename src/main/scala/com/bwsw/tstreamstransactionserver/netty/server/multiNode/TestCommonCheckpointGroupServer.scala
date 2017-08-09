package com.bwsw.tstreamstransactionserver.netty.server.multiNode


import com.bwsw.tstreamstransactionserver.netty.server.{Notifier, RocksWriter, TestRocksWriter}
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}

class TestCommonCheckpointGroupServer(authenticationOpts: AuthenticationOptions,
                                      zookeeperOpts: CommonOptions.ZookeeperOptions,
                                      serverOpts: BootstrapOptions,
                                      commonRoleOptions: CommonRoleOptions,
                                      checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                                      commonPrefixesOptions: CommonPrefixesOptions,
                                      bookkeeperOptions: BookkeeperOptions,
                                      storageOpts: StorageOptions,
                                      rocksStorageOpts: RocksStorageOptions,
                                      packageTransmissionOpts: TransportOptions,
                                      subscribersUpdateOptions: SubscriberUpdateOptions)
  extends CommonCheckpointGroupServer(
    authenticationOpts,
    zookeeperOpts,
    serverOpts,
    commonRoleOptions,
    checkpointGroupRoleOptions,
    commonPrefixesOptions,
    bookkeeperOptions,
    storageOpts,
    rocksStorageOpts,
    packageTransmissionOpts,
    subscribersUpdateOptions){

  override protected lazy val rocksWriter: RocksWriter =
    new TestRocksWriter(
      rocksStorage,
      transactionDataService,
      producerNotifier,
      consumerNotifier
    )
  private lazy val producerNotifier =
    new Notifier[ProducerTransaction]
  private lazy val consumerNotifier =
    new Notifier[ConsumerTransaction]

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
