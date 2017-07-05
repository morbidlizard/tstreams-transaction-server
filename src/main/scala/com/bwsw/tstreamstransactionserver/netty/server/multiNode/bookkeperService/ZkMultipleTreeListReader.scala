package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}
import com.bwsw.tstreamstransactionserver.netty.server.RecordType

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
          .map(id =>
            if (storageManager.isClosed(id))
              LedgerIDAndItsLastRecordID(id, NoRecordRead)
            else
              LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead)
          )
          .getOrElse(LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead))
      )
    }
  }

  private type Timestamp = Long
  @throws[Throwable]
  private def findMaxAvailableLastRecordTimestamp(ledgersToProcess: Array[Array[RecordWithIndex]]): Option[Timestamp] = {
    ledgersToProcess.foldLeft(Option.empty[Timestamp])((timestampOpt, recordsWithIndexes) => {
      timestampOpt.flatMap { timestamp =>
        recordsWithIndexes.lastOption.map(recordWithIndex =>
          recordWithIndex.record.timestamp min timestamp
        ).orElse(Some(timestamp))
      }.orElse(recordsWithIndexes.lastOption.map(_.record.timestamp))
    })
  }

  private def getLedgerIDAndItsOrderedRecords(ledgersAndTheirLastRecordsToProcess: Array[LedgerIDAndItsLastRecordID]) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledgerMetaInfo =>
        if (ledgerMetaInfo.ledgerID == NoLedgerExist)
          (Array.empty[RecordWithIndex], ledgerMetaInfo)
        else {
          storageManager
            .openLedger(ledgerMetaInfo.ledgerID)
            .map { ledgerHandle =>
              val recordsWithIndexes =
                ledgerHandle.getOrderedRecords(ledgerMetaInfo.ledgerLastRecordID + 1)

              (recordsWithIndexes, ledgerMetaInfo)
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
          .openLedger(ledgerMetaInfo.ledgerID)
          .filter(ledgerHandle => storageManager.isClosed(ledgerHandle.id))
          .map { ledgerHandle =>
            if (zkTreeList.lastEntityID.get == ledgerMetaInfo.ledgerID &&
              ledgerHandle.lastRecordID() == ledgerMetaInfo.ledgerLastRecordID
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
      case (metainfo, zkTreeList) =>
        storageManager
          .openLedger(metainfo.ledgerID)
          .flatMap { ledgerHandle =>
            zkTreeList.lastEntityID.map(ledgerID =>
              if (ledgerID != metainfo.ledgerID &&
                ledgerHandle.lastRecordID() == metainfo.ledgerLastRecordID
              ) {
                val newLedgerID = zkTreeList
                  .getNextNode(metainfo.ledgerID)
                  .getOrElse(throw new
                      IllegalStateException(
                        s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerHandle.id}"
                      )
                  )
                if (storageManager.isClosed(newLedgerID))
                  LedgerIDAndItsLastRecordID(newLedgerID, NoRecordRead)
                else
                  metainfo
              } else {
                metainfo
              }
            )
          }.getOrElse(metainfo)
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

    val nextRecordsAndLedgersToProcess: Array[LedgerIDAndItsLastRecordID] =
      getNextLedgersIfNecessary(
        getRecordsToStartWith(processedLastRecordIDsAcrossLedgersCopy)
      )

    val ledgersForNextProcessingIndexes =
      excludeProcessedLastLedgersIfTheyWere(nextRecordsAndLedgersToProcess)
        .toArray


    if (
      nextRecordsAndLedgersToProcess.contains(LedgerIDAndItsLastRecordID(NoLedgerExist, NoRecordRead)) ||
        nextRecordsAndLedgersToProcess.length != ledgersForNextProcessingIndexes.length
    ) {
      (Array.empty[Record], processedLastRecordIDsAcrossLedgersCopy)
    }
    else {
      val ledgersToProcess =
        ledgersForNextProcessingIndexes.map(index =>
          nextRecordsAndLedgersToProcess(index)
        )

      val (ledgersRecords, ledgersAndTheirLastRecordIDs) =
        getLedgerIDAndItsOrderedRecords(
          ledgersToProcess
        ).unzip

      val timestampOpt: Option[Long] =
        findMaxAvailableLastRecordTimestamp(ledgersRecords)

      val (records, processedLedgersAndRecords) = timestampOpt
        .map { timestamp =>

          val (records, ledgersIDsAndTheirRecordIDs) =
            (ledgersRecords zip ledgersAndTheirLastRecordIDs).map { case (ledgerRecords, ledgerIDRecordID) =>
              val recordsWithIndexes =
                ledgerRecords.takeWhile(_.record.timestamp <= timestamp)

              val lastRecordID = recordsWithIndexes.lastOption
                .map(_.index).getOrElse(ledgerIDRecordID.ledgerLastRecordID)

              (recordsWithIndexes.map(_.record),
                LedgerIDAndItsLastRecordID(ledgerIDRecordID.ledgerID, lastRecordID))
            }.unzip

          (records.flatten, ledgersIDsAndTheirRecordIDs)
        }
        .getOrElse((Array.empty[Record], ledgersToProcess))

      (records,
        orderLedgers(
          nextRecordsAndLedgersToProcess,
          processedLedgersAndRecords,
          ledgersForNextProcessingIndexes
        )
      )
    }
  }
}
