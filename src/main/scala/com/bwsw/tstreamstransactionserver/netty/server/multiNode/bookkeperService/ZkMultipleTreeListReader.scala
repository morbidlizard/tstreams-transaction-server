package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.MetadataRecord

import ZkMultipleTreeListReader.{key, NoLedgerExist, NoRecordRead}

object ZkMultipleTreeListReader {
  val key = "lastKey".getBytes

  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L

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
        val ledgerFirstLastRecord  = ledgerLastRecord(firstLedger)
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

class ZkMultipleTreeListReader(zkTreeLists: Array[ZookeeperTreeListLong],
                               storageManager: StorageManager,
                               keyValueDatabaseManager: KeyValueDatabaseManager,
                               databaseIndex: Int) {

  @throws[IllegalArgumentException]
  private def getRecordsToStartWith(databaseData: Option[Array[Byte]]): Array[LedgerIDAndItsLastRecordID] = {
    databaseData match {
      case Some(bytes) =>
        val stateOfProcessing = MetadataRecord.fromByteArray(bytes)
        require(
          stateOfProcessing.records.length == zkTreeLists.length,
          "Number of trees have been changed since last processing!"
        )
        stateOfProcessing.records.map(record =>
          LedgerIDAndItsLastRecordID(
            record.ledgerID,
            record.LedgerLastRecordID
          )
        )
      case None =>
        zkTreeLists.map(zkTreeList =>
          zkTreeList.firstEntityID
            .map(id => LedgerIDAndItsLastRecordID(id, NoRecordRead))
            .orElse(Some(LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead)))
            .get
        )
    }
  }

  private def ledgerLastRecord(ledger: Ledger): Record = {
    Record.fromByteArray(ledger.readEntry(ledger.lastEntryID()))
  }
  
  private def getAllRecordsOrderedUntilTimestampMet(from: Long,
                                                    ledger: Ledger,
                                                    timestamp: Long) = {
    val fromCorrected =
      if (from < 0L) 0L
      else from

    val lastRecordID = ledger.lastEntryID()
    val indexes = fromCorrected to lastRecordID

    ledger.readRange(fromCorrected, lastRecordID)
      .map(binaryEntry => Record.fromByteArray(binaryEntry))
      .zip(indexes).sortBy(_._1.timestamp)
      .takeWhile(_._1.timestamp <= timestamp)
  }


  private def getLastRecordIDEqOrLsTimestamp(records: Seq[(Record, Long)]) = {
    records.lastOption.map(_._2).getOrElse(NoRecordRead)
  }

  private def determineTimestampUntilProcess(ledgersToProcess: Array[LedgerIDAndItsLastRecordID]): Long = {
    ledgersToProcess.foldLeft(Long.MaxValue)((acc, ledgerAndRecord) =>
      if (ledgerAndRecord.ledgerID == NoLedgerExist) {
        acc
      }
      else {
        val timestamp = storageManager
          .getLedger(ledgerAndRecord.ledgerID)
          .map(ledgerID => ledgerLastRecord(ledgerID))
          .map{record =>
            require(
              record.recordType == RecordType.Timestamp,
              "All ledgers must have their last record as type of 'Timestamp'"
            )
            record.timestamp
          }
          .getOrElse(throw new
              IllegalStateException(
                s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerAndRecord.ledgerID}"
              )
          )
        acc min timestamp
      }
    )
  }

  private def getOrderedRecordsByLedgerAndItsLastRecordIDBeforeTimestamp(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID],
                                                                         timestamp: Long) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledger =>
        if (ledger.ledgerID == NoLedgerExist)
          (Array.empty[Record],
            LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead)
          )
        else {
          val records = storageManager
            .getLedger(ledger.ledgerID)
            .map(ledgerID => getAllRecordsOrderedUntilTimestampMet(ledger.recordID, ledgerID, timestamp))
            .getOrElse(throw new
                IllegalStateException(
                  s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledger.ledgerID}"
                )
            )
          val lastRecordIDProcessed = getLastRecordIDEqOrLsTimestamp(records)
          val orderedRecords = records.map(_._1)
          (orderedRecords,
            LedgerIDAndItsLastRecordID(ledger.ledgerID, lastRecordIDProcessed)
          )
        })
  }

  def process(): Unit = {
    val data: Option[Array[Byte]] = Option(keyValueDatabaseManager
      .getRecordFromDatabase(
        databaseIndex,
        ZkMultipleTreeListReader.key
      )
    )

    val ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID] =
      getRecordsToStartWith(data)

    val timestamp: Long =
      determineTimestampUntilProcess(ledgersAndTheirLastRecordsToProcess)

    if (timestamp == Long.MaxValue)
    {
      Array.empty[Record]
    }
    else {
      val c = getOrderedRecordsByLedgerAndItsLastRecordIDBeforeTimestamp(
        ledgersAndTheirLastRecordsToProcess,
        timestamp
      )

      c.foreach(x => println(x._2))
    }
  }
}
