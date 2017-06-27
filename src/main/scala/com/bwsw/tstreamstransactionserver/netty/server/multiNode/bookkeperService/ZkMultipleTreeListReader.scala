package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}

private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(zkTreeLists: Array[ZookeeperTreeListLong],
                               storageManager: StorageManager) {

  @throws[IllegalArgumentException]
  private def getRecordsToStartWith(databaseData: Array[LedgerIDAndItsLastRecordID]): Array[LedgerIDAndItsLastRecordID] = {
    if (databaseData.nonEmpty) {
      require(
        databaseData.length == zkTreeLists.length,
        "Number of trees has been changed since last processing!"
      )
      databaseData
    }
    else {
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
    val fromCorrected = from + 1L

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
          (Array.empty[Record], ledgerMetaInfo)
        else {
          storageManager
            .getLedger(ledgerMetaInfo.ledgerID)
            .map { ledger =>
              val records = getAllRecordsOrderedUntilTimestampMet(ledgerMetaInfo.ledgerLastRecordID, ledger, timestamp)
              val lastRecordIDProcessed = getLastRecordIDEqOrLsTimestamp(records)
              val orderedRecords = records.map(_._1)
              (orderedRecords,
                LedgerIDAndItsLastRecordID(ledgerMetaInfo.ledgerID, lastRecordIDProcessed)
              )
            }
            .getOrElse(throw new
                IllegalStateException(
                  s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerMetaInfo.ledgerID}"
                )
            )
        }
      )
  }

  private def excludeFinalState(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID]): List[Int] = {
    (ledgersAndTheirLastRecordsToProcess zip zkTreeLists zipWithIndex)
      .foldRight(List.empty[Int]) { case (((ledgerMetaInfo, zkTreeList), index), acc) =>
        storageManager
          .getLedger(ledgerMetaInfo.ledgerID)
          .map { ledger =>
            if (zkTreeList.lastEntityID.get == ledgerMetaInfo.ledgerID &&
              ledger.lastEntryID() == ledgerMetaInfo.ledgerLastRecordID
            ) {
              acc
            } else {
              index :: acc
            }
          }
          .getOrElse(acc)
      }
  }

  private def orderLedgers(ledgers1: Array[LedgerIDAndItsLastRecordID],
                           ledgers2: Array[LedgerIDAndItsLastRecordID],
                           indexes: Seq[Int]) = {
    val map = (indexes zip ledgers2).toMap
    indexes foreach(index => ledgers1.update(index, map(index)))
    ledgers1
  }

  def process(data: Array[LedgerIDAndItsLastRecordID]): (Array[Record], Array[LedgerIDAndItsLastRecordID]) = {
    val ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID] =
      getRecordsToStartWith(data)

    val ledgersIndexesToProcess =
      excludeFinalState(ledgersAndTheirLastRecordsToProcess)

    val ledgersToProcess =
      ledgersIndexesToProcess.map(index =>
        ledgersAndTheirLastRecordsToProcess(index)
      ).toArray

    val timestampOpt: Option[Long] =
      determineTimestampUntilProcess(ledgersToProcess)

    val result = timestampOpt
      .map { timestamp =>
        val (records, ledgerRecordIDs) =
          getOrderedRecordsByLedgerAndItsLastRecordIDBeforeTimestamp(
            ledgersToProcess,
            timestamp
          ).unzip

        val orderedRecords = records.flatten.sorted

        (orderedRecords, ledgerRecordIDs)
      }
      .orElse(
        Some((Array.empty[Record], ledgersAndTheirLastRecordsToProcess))
      )
      .get

    (result._1, orderLedgers(ledgersAndTheirLastRecordsToProcess, result._2, ledgersIndexesToProcess))
  }
}
