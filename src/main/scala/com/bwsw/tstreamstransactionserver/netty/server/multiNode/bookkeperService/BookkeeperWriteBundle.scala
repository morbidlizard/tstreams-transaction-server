package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService


class BookkeeperWriteBundle(val bookkeeperMaster: BookkeeperMaster,
                            timeBetweenCreationOfLedgersMs: Int) {
  private val masterTask =
    new Thread(
      bookkeeperMaster,
      "bookkeeper-master-%d"
    )

  def start(): Unit = {
    masterTask.start()
  }


  def stop(): Unit = {
    masterTask.interrupt()
  }
}
