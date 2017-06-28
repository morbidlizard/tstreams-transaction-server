package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType, RecordWithIndex}
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

  private def getLastRecordID(records: Seq[RecordWithIndex]) = {
    records.lastOption.map(_.index).getOrElse(NoRecordRead)
  }

  @throws[Throwable]
  private def findMaxAvailableLastRecordTimestamp(ledgersToProcess: Array[LedgerIDAndItsLastRecordID]): Option[Long] = {
    ledgersToProcess.foldLeft(Option.empty[Long])((timestampOpt, ledgerAndRecord) =>
      if (ledgerAndRecord.ledgerID == NoLedgerExist) {
        timestampOpt
      }
      else {
        val timestamp = storageManager
          .getLedger(ledgerAndRecord.ledgerID)
          .map(ledgerHandle => ledgerHandle.lastEntry().get)
          .map{record =>
            require(
              record.recordType == RecordType.Timestamp,
              "All ledgers must have their last record as type of 'Timestamp'"
            )
            record.timestamp
          }
          .getOrElse(throw new
              IllegalStateException(
                s"There is problem with storage - there is no such ledger ${ledgerAndRecord.ledgerID}"
              )
          )

        timestampOpt
          .map(_ min timestamp)
          .orElse(Some(timestamp))
      }
    )
  }

  private def getOrderedRecordsAcrossLedgersAndTheirLastRecordIDs(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID],
                                                                  timestamp: Long) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledgerMetaInfo =>
        if (ledgerMetaInfo.ledgerID == NoLedgerExist)
          (Array.empty[Record], ledgerMetaInfo)
        else {
          storageManager
            .getLedger(ledgerMetaInfo.ledgerID)
            .map { ledger =>
              val recordsWithIndexes =
                ledger.getAllRecordsOrderedUntilTimestampMet(ledgerMetaInfo.ledgerLastRecordID, timestamp)
              val lastRecordIDProcessed =
                getLastRecordID(recordsWithIndexes)
              val orderedRecords =
                recordsWithIndexes.map(_.record)

              (orderedRecords,
                LedgerIDAndItsLastRecordID(ledgerMetaInfo.ledgerID, lastRecordIDProcessed)
              )
            }
            .getOrElse(throw new
                IllegalStateException(
                  s"There is problem with storage - there is no such ledger ${ledgerMetaInfo.ledgerID}"
                )
            )
        }
      )
  }

  private def excludeProcessedLastLedgersIfTheyWere(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID]): List[Int] = {
    ledgersAndTheirLastRecordsToProcess.zip(zkTreeLists).zipWithIndex
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

  private def getNextLedgersIfNecessary(ledgersAndTheirLastRecords: Array[LedgerIDAndItsLastRecordID]) = {
    (ledgersAndTheirLastRecords zip zkTreeLists).map {
      case (ledgerAndItsLastRecord, zkTreeList) =>
        storageManager
          .getLedger(ledgerAndItsLastRecord.ledgerID)
          .flatMap { ledger => zkTreeList.lastEntityID.map(lastEntityID =>
              if (lastEntityID != ledgerAndItsLastRecord.ledgerID &&
                ledger.lastEntryID() == ledgerAndItsLastRecord.ledgerLastRecordID
              ) {
                val newLedgerID = zkTreeList
                  .getNextNode(ledgerAndItsLastRecord.ledgerID)
                  .getOrElse(throw new
                    IllegalStateException(
                      s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledger.id}"
                    )
                  )
                LedgerIDAndItsLastRecordID(newLedgerID, NoRecordRead)
              } else {
                ledgerAndItsLastRecord
              }
            )
          }.getOrElse(ledgerAndItsLastRecord)
    }
  }

  private def orderLedgers(ledgersOutOfProcessing: Array[LedgerIDAndItsLastRecordID],
                           processedLedgers: Array[LedgerIDAndItsLastRecordID],
                           processedLedgersIndexes: Array[Int]) = {
    val indexToProcessedLedgerMap = (processedLedgersIndexes zip processedLedgers).toMap
    processedLedgersIndexes.foreach(index =>
      ledgersOutOfProcessing.update(index, indexToProcessedLedgerMap(index))
    )
    ledgersOutOfProcessing
  }

  def process(ledgersAndTheirLastRecordIDsProcessed: Array[LedgerIDAndItsLastRecordID]): (Array[Record], Array[LedgerIDAndItsLastRecordID]) = {
    val ledgersAndTheirLastRecordIDsProcessedCopy =
      java.util.Arrays.copyOf(
        ledgersAndTheirLastRecordIDsProcessed,
        ledgersAndTheirLastRecordIDsProcessed.length
      )

    val ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID] =
      getNextLedgersIfNecessary(
        getRecordsToStartWith(ledgersAndTheirLastRecordIDsProcessedCopy)
      )

    val processedLedgersIndexes =
      excludeProcessedLastLedgersIfTheyWere(ledgersAndTheirLastRecordsToProcess)
        .toArray

    val ledgersToProcess =
      processedLedgersIndexes.map(index =>
        ledgersAndTheirLastRecordsToProcess(index)
      )

    val timestampOpt: Option[Long] =
      findMaxAvailableLastRecordTimestamp(ledgersToProcess)

    val (records, processedLedgers) = timestampOpt
      .map { timestamp =>
        val (records, ledgersIDsAndItsRecordIDs) =
          getOrderedRecordsAcrossLedgersAndTheirLastRecordIDs(
            ledgersToProcess,
            timestamp
          ).unzip

        val orderedRecords = records.flatten.sorted

        (orderedRecords, ledgersIDsAndItsRecordIDs)
      }
      .getOrElse((Array.empty[Record], ledgersToProcess))

    (records,
      orderLedgers(
        ledgersAndTheirLastRecordsToProcess,
        processedLedgers,
        processedLedgersIndexes
      )
    )
  }
}
