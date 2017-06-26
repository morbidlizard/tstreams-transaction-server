package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType}

class ZkMultipleTreeListReader(zkTreeLists: Seq[ZookeeperTreeListLong])
{
  zkTreeLists.foreach(zkTreeList => zkTreeList)

}

object ZkMultipleTreeListReader {
  def ledgerLastRecord(ledger: Ledger): Record = {
    Record.fromByteArray(ledger.readEntry(ledger.lastEntryID()))
  }

  def determineRightBoundProcessUntil(records: Seq[Record]) = {
    require(
      records.forall(_.recordType == RecordType.Timestamp),
      "All ledgers must have their last record as type of 'Timestamp'"
    )
    records.minBy(record => record.timestamp).timestamp
  }

  def getAllRecordsOrderedUntilTimestampMet(ledger: Ledger,
                                            timestamp: Long) = {
    ledger.readRange(0L, ledger.lastEntryID())
      .map(Record.fromByteArray)
      .zipWithIndex.sortBy(_._1.timestamp)
      .takeWhile(_._1.timestamp <= timestamp)
  }

  def getLastRecordIDEqOrLsTimestamp(records: Seq[(Record, Int)]) = {
    records.lastOption.map(_._2).getOrElse(-1L)
  }


  def processTwoLedgers(storage: StorageManager) = {
    val ledgerFirstOpt  = storage.getLedger(0L)
    val secondLedgerOpt = storage.getLedger(1L)

    (ledgerFirstOpt, secondLedgerOpt) match {
      case (Some(firstLedger),Some(secondLedger)) =>
        val ledgerFirstLastRecord = ledgerLastRecord(firstLedger)
        val ledgerSecondLastRecord = ledgerLastRecord(secondLedger)

        val readUpToTimestamp = determineRightBoundProcessUntil(
          Array(ledgerFirstLastRecord, ledgerSecondLastRecord)
        )

        val ledgerFirstRecords =
          getAllRecordsOrderedUntilTimestampMet(
            firstLedger,
            readUpToTimestamp
          )

        val ledgerSecondRecords =
          getAllRecordsOrderedUntilTimestampMet(
            secondLedger,
            readUpToTimestamp
          )

        val recordsToProcess = (ledgerFirstRecords.map(_._1) ++ ledgerSecondRecords.map(_._1))
          .sortBy(_.timestamp)

        val ledgerFirstLastLedgerRecordIDAfterTimestampApplied =
          getLastRecordIDEqOrLsTimestamp(ledgerFirstRecords)


        val ledgerSecondLastLedgerRecordIDAfterTimestampApplied =
          getLastRecordIDEqOrLsTimestamp(ledgerSecondRecords)

        println(
          ledgerFirstLastLedgerRecordIDAfterTimestampApplied,
          ledgerSecondLastLedgerRecordIDAfterTimestampApplied
        )

    }
  }
}
