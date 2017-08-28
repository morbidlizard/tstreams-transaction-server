package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{BookkeeperToRocksWriter, ZkMultipleTreeListReader, LongZookeeperTreeList}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookkeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper

class BookkeeperSlave(bookKeeper: BookKeeper,
                      bookkeeperOptions: BookkeeperOptions,
                      zkTrees: Array[LongZookeeperTreeList],
                      commitLogService: CommitLogService,
                      rocksWriter: RocksWriter)
  extends Runnable {

  private val bookkeeperToRocksWriter = {
    val bk =
      new BookkeeperWrapper(
        bookKeeper,
        bookkeeperOptions
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