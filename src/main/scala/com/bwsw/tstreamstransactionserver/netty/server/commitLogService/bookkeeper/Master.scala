package com.bwsw.tstreamstransactionserver.netty.server.commitLogService.bookkeeper

import java.util.concurrent.BlockingQueue

import org.apache.bookkeeper.client.{BKException, BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import Utils._
import org.apache.bookkeeper.client.BookKeeper.DigestType

import scala.annotation.tailrec

class Master(client: CuratorFramework,
             bookKeeper: BookKeeper,
             master: ServerRole,
             ledgerLogPath: String,
             password: Array[Byte],
             timeBetweenCreationOfLedgers: Int,
             openedLedgers: BlockingQueue[LedgerHandle]
            )
{
  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2


  def lead(skipPast: EntryId): EntryId = {
    val ledgersWithMetadataInformation =
      retrieveAllLedgersFromZkServer

    val (ledgerIDs, stat, mustCreate) = (
      ledgersWithMetadataInformation.ledgers,
      ledgersWithMetadataInformation.zNodeMetadata,
      ledgersWithMetadataInformation.mustCreate
    )

    val newLedgers: Stream[Long] =
      processNewLedgersThatHaventSeenBefore(ledgerIDs, skipPast)
        .toStream

    val newLedgerHandles =
      openLedgersHandlers(newLedgers, BookKeeper.DigestType.MAC)
        .toList

    val lastDisplayedEntry: EntryId =
      traverseLedgersRecords(
        newLedgerHandles,
        EntryId(skipPast.ledgerId, skipPast.entryId + 1),
        skipPast,
        (entry, data) =>{
          println(s"" +
            s"Ledger = ${entry.ledgerId}, " +
            s"RecordID = ${entry.entryId}, " +
            s"Value = ${bytesToIntsArray(data).head}, " +
            "catchup"
          )
        }
      )

    whileLeaderDo(stat.getVersion, ledgerIDs)

    lastDisplayedEntry
  }

  private def retrieveAllLedgersFromZkServer: LedgersWithMetadataInformation = {
    val zNodeMetadata: Stat = new Stat()
    scala.util.Try {
      val binaryData = client.getData
        .storingStatIn(zNodeMetadata)
        .forPath(ledgerLogPath)
      val ledgers = bytesToLongsArray(binaryData)

      ledgers
    } match {
      case scala.util.Success(ledgers) =>
        LedgersWithMetadataInformation(ledgers, zNodeMetadata, mustCreate = false)
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NoNodeException =>
          LedgersWithMetadataInformation(Array.emptyLongArray, zNodeMetadata, mustCreate = true)
        case _ =>
          throw throwable
      }
    }
  }

  private def processNewLedgersThatHaventSeenBefore(ledgers: Array[Long],
                                                    skipPast: EntryId) = {
    if (skipPast.ledgerId != noLeadgerId) {
      val ledgerToStartWith = ledgers.indexWhere(id => id >= skipPast.ledgerId)
      ledgers.slice(ledgerToStartWith, ledgers.length)
    }
    else
      ledgers
  }


  private def openLedgersHandlers(ledgers: Stream[Long],
                                  digestType: DigestType
                                 ) = {
    ledgers
      .map(ledgerID =>
        scala.util.Try(
          bookKeeper.openLedger(
            ledgerID,
            digestType,
            password
          )))
      .takeWhile {
        case scala.util.Success(_) =>
          true
        case scala.util.Failure(throwable) => throwable match {
          case _: BKException.BKLedgerRecoveryException =>
            false
          case _: Throwable =>
            throw throwable
        }
      }
      .map(_.get)
  }

  @tailrec
  private def traverseLedgersRecords(ledgerHandlers: List[LedgerHandle],
                                     nextEntry: EntryId,
                                     lastDisplayedEntry: EntryId,
                                     processNewData: (EntryId, Array[Byte]) => Unit
                                    ): EntryId =
    ledgerHandlers match {
      case Nil =>
        lastDisplayedEntry

      case ledgeHandle :: handles =>
        if (nextEntry.entryId > ledgeHandle.getLastAddConfirmed) {
          val startEntry = EntryId(ledgeHandle.getId, 0)
          traverseLedgersRecords(handles, startEntry, lastDisplayedEntry, processNewData)
        }
        else {
          val entries = ledgeHandle.readEntries(
            nextEntry.entryId,
            ledgeHandle.getLastAddConfirmed
          )

          var newLastDisplayedEntry = lastDisplayedEntry
          while (entries.hasMoreElements) {
            val entry = entries.nextElement
            val entryData = entry.getEntry
            newLastDisplayedEntry = EntryId(ledgeHandle.getId, entry.getEntryId)
            processNewData(newLastDisplayedEntry, entryData)
          }
          traverseLedgersRecords(handles, nextEntry, newLastDisplayedEntry, processNewData)
        }
    }

  private def ledgerHandleToWrite(ensembleNumber: Int,
                                  writeQuorumNumber: Int,
                                  ackQuorumNumber: Int,
                                  digestType: DigestType
                                 ) = {
    bookKeeper.createLedger(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      digestType,
      password
    )
  }


  private def createLedgersLog(ledgersIDsBinary: Array[Byte]) =
  {
    scala.util.Try(
      client.create.forPath(ledgerLogPath, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NodeExistsException =>
        case _ => throw throwable
      }
    }
  }

  private def updateLedgersLog(ledgersIDsBinary: Array[Byte],
                               logVersion: Int) =
  {
    scala.util.Try(
      client.setData()
        .withVersion(logVersion)
        .forPath(ledgerLogPath, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.BadVersionException =>
        case _ =>
          throw throwable
      }
    }
  }

  private final def whileLeaderDo(logVersion: Int,
                                  previousLedgers: Array[Long]
                                 ) = {

    var lastAccessTimes = System.currentTimeMillis()
    @tailrec
    def onBeingLeaderDo(logVersion: Int,
                        previousLedgers: Array[Long]
                       ): Unit = {
      if (master.hasLeadership) {
        if ((lastAccessTimes - System.currentTimeMillis()) <= timeBetweenCreationOfLedgers) {
          lastAccessTimes = System.currentTimeMillis()
          onBeingLeaderDo(
            logVersion,
            previousLedgers
          )
        }
        else {
          val ledgerHandle = ledgerHandleToWrite(
            ensembleNumber,
            writeQuorumNumber,
            ackQuorumNumber,
            BookKeeper.DigestType.MAC
          )

          val previousLedgersWithNewOne = previousLedgers :+ ledgerHandle.getId
          val ledgersIDsToBytes = longArrayToBytes(previousLedgersWithNewOne)
          if (logVersion == 0) {
            createLedgersLog(ledgersIDsToBytes)
          } else {
            updateLedgersLog(ledgersIDsToBytes, logVersion)
          }

          openedLedgers.add(ledgerHandle)

          lastAccessTimes = System.currentTimeMillis()
          onBeingLeaderDo(
            logVersion + 1,
            previousLedgersWithNewOne
          )
        }
      }
    }
    onBeingLeaderDo(logVersion, previousLedgers)
  }
}
