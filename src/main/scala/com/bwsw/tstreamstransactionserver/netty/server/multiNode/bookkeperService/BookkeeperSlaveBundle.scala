package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder

class BookkeeperSlaveBundle(bookkeeperSlave: BookkeeperSlave,
                            timeBetweenCreationOfLedgersMs: Int) {

  private val bookKeeperExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("bookkeeper-slave-%d").build()
    )

  private lazy val futureTask =
    bookKeeperExecutor.scheduleWithFixedDelay(
      bookkeeperSlave,
      0L,
      timeBetweenCreationOfLedgersMs,
      TimeUnit.NANOSECONDS
    )

  def start(): Unit = {
    futureTask
  }


  def stop(): Unit = {
    futureTask.cancel(true)
    bookKeeperExecutor.shutdown()
    scala.util.Try {
      bookKeeperExecutor.awaitTermination(
        0L,
        TimeUnit.NANOSECONDS
      )
    }
  }

}
