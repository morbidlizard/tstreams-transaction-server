package com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService

import java.util.concurrent.BlockingQueue

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.Utils._
import org.apache.bookkeeper.client.BookKeeper.DigestType
import org.apache.bookkeeper.client.{BKException, BookKeeper, LedgerHandle}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat

import scala.annotation.tailrec

class Master(client: CuratorFramework,
             bookKeeper: BookKeeper,
             master: ServerRole,
             replicationConfig: ReplicationConfig,
             ledgerLogPath: String,
             password: Array[Byte],
             timeBetweenCreationOfLedgers: Int,
             openedLedgers: BlockingQueue[LedgerHandle],
             closedLedgers: BlockingQueue[LedgerHandle]
            )
{

  def lead(skipPast: LedgerID): LedgerID = {
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

    val lastDisplayedLedgerID: LedgerID =
      traverseLedgersRecords(
        newLedgerHandles,
        skipPast
      )

    whileLeaderDo(stat.getVersion, ledgerIDs)

    lastDisplayedLedgerID
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
                                                    skipPast: LedgerID) = {
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
                                     lastDisplayedEntry: LedgerID): LedgerID =
    ledgerHandlers match {
      case Nil =>
        lastDisplayedEntry

      case ledgerHandle :: handles =>
        val lastProcessedLedger =
          if (ledgerHandle.isClosed && (lastDisplayedEntry.ledgerId < ledgerHandle.getId)) {
            closedLedgers.add(ledgerHandle)
            LedgerID(ledgerHandle.getId)
          }
          else {
            lastDisplayedEntry
          }

        traverseLedgersRecords(handles, lastProcessedLedger)
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
      client.create
        .creatingParentsIfNeeded()
        .forPath(ledgerLogPath, ledgersIDsBinary)
    ) match {
      case scala.util.Success(_) =>
      case scala.util.Failure(throwable) => throwable match {
        case _: KeeperException.NodeExistsException =>
        case _ =>
          throw throwable
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

    var lastAccessTimes = 0L
    @tailrec
    def onBeingLeaderDo(logVersion: Int,
                        previousLedgers: Array[Long]
                       ): Unit = {
      if (master.hasLeadership) {
        if ((System.currentTimeMillis() - lastAccessTimes) <= timeBetweenCreationOfLedgers) {
          onBeingLeaderDo(
            logVersion,
            previousLedgers
          )
        }
        else {
          lastAccessTimes = System.currentTimeMillis()

          val ledgerHandle = ledgerHandleToWrite(
            replicationConfig.ensembleNumber,
            replicationConfig.writeQuorumNumber,
            replicationConfig.ackQuorumNumber,
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
