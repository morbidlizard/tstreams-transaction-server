package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.batch.{BigCommit, BigCommitWithFrameParser}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperRecordFrame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService


class BookkeeperToRocksWriter(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                              commitLogService: CommitLogService,
                              rocksWriter: RocksWriter)
  extends Runnable
{
  private def getBigCommit(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): BigCommitWithFrameParser = {
    val value = MetadataRecord(processedLastRecordIDsAcrossLedgers).toByteArray
    val bigCommit = new BigCommit(rocksWriter, RocksStorage.BOOKKEEPER_LOG_STORE, BigCommit.bookkeeperKey, value)
    new BigCommitWithFrameParser(bigCommit)
  }

  def processAndPersistRecords(): PersistedCommitAndMoveToNextRecordsInfo = {
    val ledgerRecordIDs = commitLogService
      .getLastProcessedLedgersAndRecordIDs

    val (records, ledgerIDsAndTheirLastRecordIDs) =
      zkMultipleTreeListReader.read(ledgerRecordIDs)

    if (records.isEmpty) {
      PersistedCommitAndMoveToNextRecordsInfo(
        isCommitted = true,
        doReadNextRecords = false
      )
    }
    else {
      val bigCommit = getBigCommit(ledgerIDsAndTheirLastRecordIDs)
      val frames = records.map(record => new BookkeeperRecordFrame(record))
      bigCommit.addFrames(frames)
      val result = PersistedCommitAndMoveToNextRecordsInfo(
        isCommitted = bigCommit.commit(),
        doReadNextRecords = true
      )

      rocksWriter.createAndExecuteTransactionsToDeleteTask(
        frames.lastOption
          .map(_.timestamp)
          .getOrElse(System.currentTimeMillis())
      )
      rocksWriter.clearProducerTransactionCache()
      result
    }
  }

  override def run(): Unit = {
    var doReadNextRecords = true
    while (doReadNextRecords) {
      val info = processAndPersistRecords()
      doReadNextRecords = info.doReadNextRecords
    }
  }
}
