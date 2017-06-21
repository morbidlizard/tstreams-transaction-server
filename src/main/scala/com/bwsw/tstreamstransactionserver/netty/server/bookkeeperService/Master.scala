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
             master: Electable,
             replicationConfig: ReplicationConfig,
             ledgerLogPath: String,
             password: Array[Byte],
             timeBetweenCreationOfLedgers: Int,
             openedLedgers: BlockingQueue[LedgerHandle])
{

  def lead(): Unit = {
    val ledgersWithMetadataInformation =
      retrieveAllLedgersFromZkServer

    val (ledgerIDs, stat, mustCreate) = (
      ledgersWithMetadataInformation.ledgers,
      ledgersWithMetadataInformation.zNodeMetadata,
      ledgersWithMetadataInformation.mustCreate
    )

    whileLeaderDo(mustCreate, ledgerIDs)
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

  private def updateLedgersLog(ledgersIDsBinary: Array[Byte]) =
  {
    val meta = client.checkExists().forPath(ledgerLogPath)
    scala.util.Try(
      client.setData()
        .withVersion(meta.getVersion)
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

  private final def whileLeaderDo(mustCreate: Boolean,
                                  previousLedgers: Array[Long]) =
  {

    var lastAccessTimes = 0L
    @tailrec
    def onBeingLeaderDo(mustCreate: Boolean,
                        previousLedgers: Array[Long]): Unit =
    {
      if (master.hasLeadership) {
        if ((System.currentTimeMillis() - lastAccessTimes) <= timeBetweenCreationOfLedgers) {
          onBeingLeaderDo(
            mustCreate,
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
          if (mustCreate) {
            createLedgersLog(ledgersIDsToBytes)
          } else {
            updateLedgersLog(ledgersIDsToBytes)
          }

          openedLedgers.add(ledgerHandle)
          onBeingLeaderDo(
            mustCreate = false,
            previousLedgersWithNewOne
          )
        }
      }
    }
    onBeingLeaderDo(mustCreate, previousLedgers)
  }
}
