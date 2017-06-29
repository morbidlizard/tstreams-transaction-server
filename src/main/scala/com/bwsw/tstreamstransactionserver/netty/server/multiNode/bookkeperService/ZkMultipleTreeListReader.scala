package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType, RecordWithIndex}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}

private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(val zkTreeLists: Array[ZookeeperTreeListLong],
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

  private def getLastRecordID(records: Array[RecordWithIndex]) = {
    records.lastOption.map(_.index).getOrElse(NoRecordRead)
  }

  private type Timestamp = Long

  @throws[Throwable]
  private def findMaxAvailableLastRecordTimestamp(ledgersToProcess: Array[LedgerIDAndItsLastRecordID]): Option[Timestamp] = {
    ledgersToProcess.foldLeft(Option.empty[Timestamp])((timestampOpt, ledgerAndRecord) =>
      if (ledgerAndRecord.ledgerID == NoLedgerExist) {
        timestampOpt
      }
      else {
        val timestamp = storageManager
          .getLedgerHandle(ledgerAndRecord.ledgerID)
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

  private def getOrderedRecordsAndLastRecordIDsAcrossLedgers(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID],
                                                             timestamp: Long) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledgerMetaInfo =>
        if (ledgerMetaInfo.ledgerID == NoLedgerExist)
          (Array.empty[Record], ledgerMetaInfo)
        else {
          storageManager
            .getLedgerHandle(ledgerMetaInfo.ledgerID)
            .map { ledgerHandle =>
              val recordsWithIndexes =
                ledgerHandle.getAllRecordsOrderedUntilTimestampMet(ledgerMetaInfo.ledgerLastRecordID, timestamp)
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
          .getLedgerHandle(ledgerMetaInfo.ledgerID)
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

  private def getNextLedgersIfNecessary(lastRecordsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]) = {
    (lastRecordsAcrossLedgers zip zkTreeLists).map {
      case (ledgerAndItsLastRecord, zkTreeList) =>
        storageManager
          .getLedgerHandle(ledgerAndItsLastRecord.ledgerID)
          .flatMap { ledgerHandle => zkTreeList.lastEntityID.map(lastEntityID =>
              if (lastEntityID != ledgerAndItsLastRecord.ledgerID &&
                ledgerHandle.lastEntryID() == ledgerAndItsLastRecord.ledgerLastRecordID
              ) {
                val newLedgerID = zkTreeList
                  .getNextNode(ledgerAndItsLastRecord.ledgerID)
                  .getOrElse(throw new
                    IllegalStateException(
                      s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerHandle.id}"
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

  def process(processedLastRecordIDsAcrossLedgers: Array[LedgerIDAndItsLastRecordID]): (Array[Record], Array[LedgerIDAndItsLastRecordID]) = {
    val processedLastRecordIDsAcrossLedgersCopy =
      java.util.Arrays.copyOf(
        processedLastRecordIDsAcrossLedgers,
        processedLastRecordIDsAcrossLedgers.length
      )

    val nextRecordsAcrossLedgersToProcess: Array[LedgerIDAndItsLastRecordID] =
      getNextLedgersIfNecessary(
        getRecordsToStartWith(processedLastRecordIDsAcrossLedgersCopy)
      )

    val notProcessedLedgersIndexes =
      excludeProcessedLastLedgersIfTheyWere(nextRecordsAcrossLedgersToProcess)
        .toArray

    val lastRecordsAcrossLedgersToProcess =
      notProcessedLedgersIndexes.map(index =>
        nextRecordsAcrossLedgersToProcess(index)
      )

    val timestampOpt: Option[Long] =
      findMaxAvailableLastRecordTimestamp(lastRecordsAcrossLedgersToProcess)

    val (records, processedLedgers) = timestampOpt
      .map { timestamp =>
        val (records, ledgersIDsAndTheirRecordIDs) =
          getOrderedRecordsAndLastRecordIDsAcrossLedgers(
            lastRecordsAcrossLedgersToProcess,
            timestamp
          ).unzip

        val orderedRecords = records.flatten.sorted

        (orderedRecords, ledgersIDsAndTheirRecordIDs)
      }
      .getOrElse((Array.empty[Record], lastRecordsAcrossLedgersToProcess))

    (records,
      orderLedgers(
        nextRecordsAcrossLedgersToProcess,
        processedLedgers,
        notProcessedLedgersIndexes
      )
    )
  }
}
