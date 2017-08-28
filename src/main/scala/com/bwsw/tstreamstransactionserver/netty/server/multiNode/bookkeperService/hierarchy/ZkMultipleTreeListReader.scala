package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerMetadata

private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(zkTreeLists: Array[ZookeeperTreeListLong],
                               storageManager: LedgerManager) {

  private type Timestamp = Long

  def read(processedLastRecordIDsAcrossLedgers: Array[LedgerMetadata]): (Array[Record], Array[LedgerMetadata]) = {
    val processedLastRecordIDsAcrossLedgersCopy =
      java.util.Arrays.copyOf(
        processedLastRecordIDsAcrossLedgers,
        processedLastRecordIDsAcrossLedgers.length
      )

    val nextRecordsAndLedgersToProcess: Array[LedgerMetadata] =
      getNextLedgersIfNecessary(
        getRecordsToStartWith(processedLastRecordIDsAcrossLedgersCopy)
      )


    val ledgersForNextProcessingIndexes =
      excludeProcessedLastLedgersIfTheyWere(nextRecordsAndLedgersToProcess)
        .toArray

    if (
      nextRecordsAndLedgersToProcess.contains(LedgerMetadata(NoLedgerExist, NoRecordRead)) ||
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
                .map(_.index).getOrElse(ledgerIDRecordID.lastRecordID)

              (recordsWithIndexes.map(_.record),
                LedgerMetadata(ledgerIDRecordID.id, lastRecordID))
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

  @throws[IllegalArgumentException]
  private def getRecordsToStartWith(databaseData: Array[LedgerMetadata]): Array[LedgerMetadata] = {
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
              LedgerMetadata(id, NoRecordRead)
            else
              LedgerMetadata(NoLedgerExist, NoRecordRead)
          )
          .getOrElse(LedgerMetadata(NoLedgerExist, NoRecordRead))
      )
    }
  }

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

  private def getLedgerIDAndItsOrderedRecords(ledgersAndTheirLastRecordsToProcess: Array[LedgerMetadata]) = {
    ledgersAndTheirLastRecordsToProcess
      .map(ledgerMetaInfo =>
        if (ledgerMetaInfo.id == NoLedgerExist)
          (Array.empty[RecordWithIndex], ledgerMetaInfo)
        else {
          storageManager
            .openLedger(ledgerMetaInfo.id)
            .map { ledgerHandle =>
              val recordsWithIndexes =
                ledgerHandle.getOrderedRecords(ledgerMetaInfo.lastRecordID + 1)

              (recordsWithIndexes, ledgerMetaInfo)
            }
            .getOrElse(throw new
                IllegalStateException(
                  s"There is problem with storage - there is no such ledger ${ledgerMetaInfo.id}"
                )
            )
        }
      )
  }

  private def excludeProcessedLastLedgersIfTheyWere(ledgersAndTheirLastRecordsToProcess: Array[LedgerMetadata]): List[Int] = {
    ledgersAndTheirLastRecordsToProcess.zip(zkTreeLists).zipWithIndex
      .foldRight(List.empty[Int]) { case (((ledgerMetaInfo, zkTreeList), index), acc) =>
        storageManager
          .openLedger(ledgerMetaInfo.id)
          .filter(ledgerHandle => storageManager.isClosed(ledgerHandle.id))
          .map { ledgerHandle =>
            if (zkTreeList.lastEntityID.get == ledgerMetaInfo.id &&
              ledgerHandle.lastRecordID() == ledgerMetaInfo.lastRecordID
            ) {
              acc
            } else {
              index :: acc
            }
          }
          .getOrElse(acc)
      }
  }

  private def getNextLedgersIfNecessary(lastRecordsAcrossLedgers: Array[LedgerMetadata]) = {
    (lastRecordsAcrossLedgers zip zkTreeLists).map {
      case (metainfo, zkTreeList) =>
        storageManager
          .openLedger(metainfo.id)
          .flatMap { ledgerHandle =>
            zkTreeList.lastEntityID.map(ledgerID =>
              if (ledgerID != metainfo.id &&
                ledgerHandle.lastRecordID() == metainfo.lastRecordID
              ) {
                val newLedgerID = zkTreeList
                  .getNextNode(metainfo.id)
                  .getOrElse(throw new
                      IllegalStateException(
                        s"There is problem with ZkTreeList - consistency of list is violated. Ledger${ledgerHandle.id}"
                      )
                  )
                if (storageManager.isClosed(newLedgerID))
                  LedgerMetadata(newLedgerID, NoRecordRead)
                else
                  metainfo
              } else {
                metainfo
              }
            )
          }.getOrElse(metainfo)
    }
  }

  private def orderLedgers(ledgersOutOfProcessing: Array[LedgerMetadata],
                           processedLedgers: Array[LedgerMetadata],
                           processedLedgersIndexes: Array[Int]) = {
    val indexToProcessedLedgerMap = (processedLedgersIndexes zip processedLedgers).toMap
    processedLedgersIndexes.foreach(index =>
      ledgersOutOfProcessing.update(index, indexToProcessedLedgerMap(index))
    )
    ledgersOutOfProcessing
  }
}
