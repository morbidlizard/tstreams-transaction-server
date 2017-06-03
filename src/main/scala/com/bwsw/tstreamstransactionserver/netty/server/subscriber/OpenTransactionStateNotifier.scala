package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.util.concurrent.atomic.AtomicLong
import com.bwsw.tstreamstransactionserver.protocol._

import scala.util.Random

final class OpenTransactionStateNotifier(observer: SubscribersObserver,
                                         notifier: SubscriberNotifier
                                        ) {
  private val uniqueMasterId =
    Random.nextInt()

  private val counters =
    new java.util.concurrent.ConcurrentHashMap[StreamPartitionUnit, AtomicLong]()

  def notifySubscribers(stream: Int,
                        partition: Int,
                        transactionId: Long,
                        count: Int,
                        status: TransactionState.Status,
                        ttlMs: Long,
                        authKey: String,
                        isNotReliable: Boolean
                       ): Unit = {
    // 1. manage next counter for (stream, part)
    val streamPartitionUnit = StreamPartitionUnit(stream, partition)
    val currentCounter = counters.computeIfAbsent(
      streamPartitionUnit, _ => new AtomicLong(0L)
    ).getAndIncrement()

    // 2. create state (open)
    val transactionState = new TransactionState(
      transactionId,
      partition,
      uniqueMasterId,
      currentCounter,
      count,
      status,
      ttlMs,
      isNotReliable,
      authKey
    )

    // 3. send it via notifier to subscribers
    observer.addSteamPartition(stream, partition)
    val subscribersOpt = observer
      .getStreamPartitionSubscribers(stream, partition)

    subscribersOpt.foreach(subscribers =>
      notifier.broadcast(subscribers, transactionState)
    )
  }
}
