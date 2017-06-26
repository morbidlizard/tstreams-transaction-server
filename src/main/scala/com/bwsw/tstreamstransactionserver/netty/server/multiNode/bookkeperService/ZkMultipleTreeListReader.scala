package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}

private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(zkTreeLists: Array[ZookeeperTreeListLong],
                               storageManager: StorageManager,
                               data: Option[Array[Byte]]) {

  @throws[IllegalArgumentException]
  private def getRecordsToStartWith(databaseData: Option[Array[Byte]]): Array[LedgerIDAndItsLastRecordID] = {
    databaseData match {
      case Some(bytes) =>
        val stateOfProcessing = MetadataRecord.fromByteArray(bytes)
        require(
          stateOfProcessing.records.length == zkTreeLists.length,
          "Number of trees has been changed since last processing!"
        )
        stateOfProcessing.records.map(record =>
          LedgerIDAndItsLastRecordID(
            record.ledgerID,
            record.ledgerLastRecordID
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

  private def determineTimestampUntilProcess(ledgersToProcess: Array[LedgerIDAndItsLastRecordID]): Option[Long] = {
    ledgersToProcess.foldLeft(Option.empty[Long])((timestampOpt, ledgerAndRecord) =>
      if (ledgerAndRecord.ledgerID == NoLedgerExist) {
        timestampOpt
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

        timestampOpt
          .map(_ min timestamp)
          .orElse(Some(timestamp))
      }
    )
  }

  private def getOrderedRecordsByLedgerAndItsLastRecordIDBeforeTimestamp(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID],
                                                                         timestamp: Long) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledgerMetaInfo =>
        if (ledgerMetaInfo.ledgerID == NoLedgerExist)
          (Array.empty[Record],
            LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead)
          )
        else {
          val records = storageManager
            .getLedger(ledgerMetaInfo.ledgerID)
            .map(ledger => getAllRecordsOrderedUntilTimestampMet(ledgerMetaInfo.ledgerLastRecordID, ledger, timestamp))
            .getOrElse(throw new
                IllegalStateException(
                  s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerMetaInfo.ledgerID}"
                )
            )
          val lastRecordIDProcessed = getLastRecordIDEqOrLsTimestamp(records)
          val orderedRecords = records.map(_._1)
          (orderedRecords,
            LedgerIDAndItsLastRecordID(ledgerMetaInfo.ledgerID, lastRecordIDProcessed)
          )
        })
  }

  def process(): (Array[Record], Array[LedgerIDAndItsLastRecordID]) = {
    val ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID] =
      getRecordsToStartWith(data)

    val timestampOpt: Option[Long] =
      determineTimestampUntilProcess(ledgersAndTheirLastRecordsToProcess)

    timestampOpt
      .map { timestamp =>
        val (records, ledgerRecordIDs) =
          getOrderedRecordsByLedgerAndItsLastRecordIDBeforeTimestamp(
            ledgersAndTheirLastRecordsToProcess,
            timestamp
          ).unzip

        val orderedRecords = records.flatten.sortBy(_.timestamp)

        (orderedRecords, ledgerRecordIDs)
      }
      .orElse(
        Some((Array.empty[Record], ledgersAndTheirLastRecordsToProcess))
      )
      .get
  }
}
