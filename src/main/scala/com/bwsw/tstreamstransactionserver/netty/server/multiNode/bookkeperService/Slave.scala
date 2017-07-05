package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{ScheduledZkMultipleTreeListReader, ZkMultipleTreeListReader, ZookeeperTreeListLong}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookKeeperWrapper
import org.apache.bookkeeper.client.BookKeeper

class Slave(bookKeeper: BookKeeper,
            replicationConfig: ReplicationConfig,
            zkTrees: Array[ZookeeperTreeListLong],
            transactionServer: TransactionServer,
            password: Array[Byte])
  extends Runnable
{

  private val scheduledZkMultipleTreeListReader = {
    val bk =
      new BookKeeperWrapper(
        bookKeeper,
        replicationConfig,
        password
      )

    val multipleTree =
      new ZkMultipleTreeListReader(
        zkTrees,
        bk
      )

    new ScheduledZkMultipleTreeListReader(
      multipleTree,
      transactionServer
    )
  }

  def follow(): Unit = {
    scheduledZkMultipleTreeListReader.run()
  }

  override def run(): Unit = {
    follow()
  }
}