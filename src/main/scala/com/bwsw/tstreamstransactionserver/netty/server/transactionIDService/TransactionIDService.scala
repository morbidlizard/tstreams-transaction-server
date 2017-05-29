package com.bwsw.tstreamstransactionserver.netty.server.transactionIDService

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

private object TransactionIDService {
  val SCALE = 100000
}

final class TransactionIDService
  extends TransactionIDGeneratable
{
  private val transactionIDAndCurrentTime = {
    val transactionGeneratorUnit =
      TransactionGeneratorUnit(1, 0L)

    new AtomicReference(transactionGeneratorUnit)
  }

  private def update(now: Long) = new UnaryOperator[TransactionGeneratorUnit] {
    override def apply(transactionGenUnit: TransactionGeneratorUnit): TransactionGeneratorUnit = {
      if (now - transactionGenUnit.currentTime > 0L) {
        TransactionGeneratorUnit(1 + 1, now)
      } else
        transactionGenUnit.copy(
          transactionID = transactionGenUnit.transactionID + 1
        )
    }
  }

  override def getTransaction(): Long = {
    val now = System.currentTimeMillis()
    val txn = transactionIDAndCurrentTime.getAndUpdate(update(now))
    getTransaction(now) + txn.transactionID
  }

  override def getTransaction(timestamp: Long): Long = {
    timestamp * TransactionIDService.SCALE
  }
}
