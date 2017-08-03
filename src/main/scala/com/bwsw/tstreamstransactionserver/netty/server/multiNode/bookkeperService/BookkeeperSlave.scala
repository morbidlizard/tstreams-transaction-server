package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{BookkeeperToRocksWriter, ZkMultipleTreeListReader, ZookeeperTreeListLong}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookkeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import org.apache.bookkeeper.client.BookKeeper

class BookkeeperSlave(bookKeeper: BookKeeper,
                      replicationConfig: ReplicationConfig,
                      zkTrees: Array[ZookeeperTreeListLong],
                      rocksReader: RocksReader,
                      rocksWriter: RocksWriter,
                      password: Array[Byte])
  extends Runnable {

  private val scheduledZkMultipleTreeListReader = {
    val bk =
      new BookkeeperWrapper(
        bookKeeper,
        replicationConfig,
        password
      )

    val multipleTree =
      new ZkMultipleTreeListReader(
        zkTrees,
        bk
      )

    new BookkeeperToRocksWriter(
      multipleTree,
      rocksReader,
      rocksWriter
    )
  }

  override def run(): Unit = {
    follow()
  }

  def follow(): Unit = {
    scheduledZkMultipleTreeListReader.run()
  }
}