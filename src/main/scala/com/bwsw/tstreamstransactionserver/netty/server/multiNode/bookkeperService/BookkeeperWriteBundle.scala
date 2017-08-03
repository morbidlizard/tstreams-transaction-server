package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

class BookkeeperWriteBundle(val commonBookkeeperCurrentLedgerAccessor: BookkeeperCurrentLedgerAccessor,
                            bookkeeperMaster: BookkeeperMaster,
                            timeBetweenCreationOfLedgersMs: Int) {
  private val masterTask =
    new Thread(
      bookkeeperMaster,
      "bookkeeper-master-%d"
    )

  private val bookKeeperExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("bookkeeper-close-ledger-scheduler-%d").build()
    )

  private lazy val futureTask =
    bookKeeperExecutor.scheduleWithFixedDelay(
      commonBookkeeperCurrentLedgerAccessor,
      0L,
      timeBetweenCreationOfLedgersMs,
      TimeUnit.MILLISECONDS
    )

  def start(): Unit = {
    masterTask.start()
    futureTask
  }


  def stop(): Unit = {
    masterTask.interrupt()
    futureTask.cancel(true)
    bookKeeperExecutor.shutdown()
    scala.util.Try {
      bookKeeperExecutor.awaitTermination(
        0L,
        TimeUnit.MILLISECONDS
      )
    }
  }
}
