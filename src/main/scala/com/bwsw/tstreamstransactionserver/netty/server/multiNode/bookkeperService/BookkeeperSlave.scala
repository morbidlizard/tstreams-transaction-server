package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{BookkeeperToRocksWriter, ZkMultipleTreeListReader, ZookeeperTreeListLong}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookkeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import org.apache.bookkeeper.client.BookKeeper

class BookkeeperSlave(bookKeeper: BookKeeper,
                      replicationConfig: ReplicationConfig,
                      zkTrees: Array[ZookeeperTreeListLong],
                      commitLogService: CommitLogService,
                      rocksWriter: RocksWriter,
                      password: Array[Byte])
  extends Runnable {

  private val bookkeeperToRocksWriter = {
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
      commitLogService,
      rocksWriter
    )
  }

  override def run(): Unit = {
    bookkeeperToRocksWriter.run()
  }
}