package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, LedgerManager}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZkMultipleTreeListReader.{NoLedgerExist, NoRecordRead}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerMetadata

private object ZkMultipleTreeListReader {
  private val NoLedgerExist: Long = -1L
  private val NoRecordRead: Long = -1L
}

class ZkMultipleTreeListReader(zkTreeLists: Array[LongZookeeperTreeList],
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

    if (
      nextRecordsAndLedgersToProcess.exists(NoLedgerExist == _.id) ||
        checkAllLedgersCanBeProcessed(nextRecordsAndLedgersToProcess)
    ) {
      (Array.empty[Record], processedLastRecordIDsAcrossLedgersCopy)
    }
    else {
      val (ledgersRecords, ledgersAndTheirLastRecordIDs) =
        getLedgerIDAndItsOrderedRecords(
          nextRecordsAndLedgersToProcess
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
        .getOrElse((Array.empty[Record], nextRecordsAndLedgersToProcess))

      (records, processedLedgersAndRecords)
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
      )
  }

  private def isZkTreeListProcessed(zkTreeList: LongZookeeperTreeList,
                                    ledgerHandle: LedgerHandle,
                                    ledgerMetadata: LedgerMetadata) = {
    zkTreeList.lastEntityID.get == ledgerMetadata.id &&
      ledgerHandle.lastRecordID() == ledgerMetadata.lastRecordID
  }

  private def checkAllLedgersCanBeProcessed(ledgersAndTheirLastRecordsToProcess: Array[LedgerMetadata]): Boolean = {
    ledgersAndTheirLastRecordsToProcess.zip(zkTreeLists)
      .foldRight(0) { case (((ledgerMetadata, zkTreeList)), acc) =>
        storageManager
          .openLedger(ledgerMetadata.id)
          .filter(ledgerHandle =>
            !isZkTreeListProcessed(
              zkTreeList,
              ledgerHandle,
              ledgerMetadata
            )
          )
          .map(_ => acc + 1)
          .getOrElse(acc)
      } != ledgersAndTheirLastRecordsToProcess.length
  }

  private def isNotReachedLastNodeAndAllCurrentLedgerRecordRead(metadata: LedgerMetadata,
                                                                ledgerHandle: LedgerHandle,
                                                                lastLedgerId: Long): Boolean = {
    lastLedgerId != metadata.id &&
      ledgerHandle.lastRecordID() == metadata.lastRecordID
  }

  private def getNextLedgersIfNecessary(lastRecordsAcrossLedgers: Array[LedgerMetadata]) = {
    (lastRecordsAcrossLedgers zip zkTreeLists).map {
      case (savedToDbLedgerMetadata, zkTreeList) =>
        val newLedgerIDOpt =
          for {
            currentLedgerHandle <- storageManager.openLedger(savedToDbLedgerMetadata.id)
            lastCreatedLedgerID <- zkTreeList.lastEntityID
            if isNotReachedLastNodeAndAllCurrentLedgerRecordRead(
              savedToDbLedgerMetadata,
              currentLedgerHandle,
              lastCreatedLedgerID
            )
          } yield {
            zkTreeList
              .getNextNode(savedToDbLedgerMetadata.id)
              .getOrElse(throw new
                  IllegalStateException(
                    s"There is problem with ZkTreeList - consistency of list is violated. Ledger${currentLedgerHandle.id}"
                  )
              )
          }

        newLedgerIDOpt
          .filter(newLedgerID =>
            storageManager.isClosed(newLedgerID)
          )
          .map(newLedgerID =>
            LedgerMetadata(newLedgerID, NoRecordRead)
          )
          .getOrElse(savedToDbLedgerMetadata)
    }
  }
}
