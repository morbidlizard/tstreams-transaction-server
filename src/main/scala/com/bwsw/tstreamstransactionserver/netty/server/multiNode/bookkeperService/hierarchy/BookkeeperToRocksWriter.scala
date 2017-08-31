package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerMetadata, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.batch.{BigCommit, BigCommitWithFrameParser}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperRecordFrame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService


class BookkeeperToRocksWriter(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                              commitLogService: CommitLogService,
                              rocksWriter: RocksWriter)
  extends Runnable {
  private def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerMetadata]): BigCommitWithFrameParser = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    val bigCommit = new BigCommit(rocksWriter, RocksStorage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
    new BigCommitWithFrameParser(bigCommit)
  }

  private var isSorted = 0L
  def processAndPersistRecords(): Boolean = {
    val ledgerRecordIDs = commitLogService
      .getLastProcessedLedgersAndRecordIDs

    val (records, ledgerIDsAndTheirLastRecordIDs) =
      zkMultipleTreeListReader.read(ledgerRecordIDs)

    if (records.isEmpty) {
      false
    }
    else {
      val bigCommit = getBigCommit(ledgerIDsAndTheirLastRecordIDs)
      val frames = records.map(record => new BookkeeperRecordFrame(record))

      val newTimestamp = frames.minBy(_.timestamp).timestamp
      if (isSorted > newTimestamp) println(s"Bad read: It was $isSorted, next $newTimestamp")
      isSorted = newTimestamp

//      println(s"[Time range]" +
//        s"${frames.minBy(_.timestamp).timestamp} to ${frames.maxBy(_.timestamp).timestamp}, ${ledgerRecordIDs.mkString(" ")}," +
//        s"${records.map(_.timestamp).sorted.mkString("[",",","]")}")
      bigCommit.addFrames(frames)
      bigCommit.commit()

      rocksWriter.createAndExecuteTransactionsToDeleteTask(
        frames.lastOption
          .map(_.timestamp)
          .getOrElse(System.currentTimeMillis())
      )
      rocksWriter.clearProducerTransactionCache()
      true
    }
  }

  override def run(): Unit = {
    var haveNextRecords = true
    while (haveNextRecords) {
      haveNextRecords = processAndPersistRecords()
    }
  }
}
