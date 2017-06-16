package ut.multiNodeServer.bookkeeper

import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.{BookkeeperGateway, LeaderSelector, ReplicationConfig, Electable}
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

import scala.collection.JavaConverters._

class BookkeeperMasterSlaveTest
  extends FlatSpec
  with Matchers
{

  private val ensembleNumber = 4
  private val writeQourumNumber = 3
  private val ackQuorumNumber = 2

  private val replicationConfig = ReplicationConfig(
    ensembleNumber,
    writeQourumNumber,
    ackQuorumNumber
  )

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber

  private val ledgerLogPath =
    "/tts/log1"

  private val passwordLedgerLogPath =
    "test".getBytes()

  private val electionPath =
    "/tts/master"

  private val createNewLedgerEveryTimeMs =
    250


  "Bookkeeper master" should "put data and Bookkeeper slave should read it" in {
    val (zkServer, zkClient, bookies) =
      Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val masterSelector = new Electable {
      override def hasLeadership: Boolean = true
      override def stopParticipateInElection(): Unit = ()
    }

    val slaveSelector = new Electable {
      override def hasLeadership: Boolean = false
      override def stopParticipateInElection(): Unit = ()
    }


    val bookkeeperGatewayMaster = new BookkeeperGateway(
      zkClient,
      masterSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val bookkeeperGatewaySlave = new BookkeeperGateway(
      zkClient,
      slaveSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val masterTask = bookkeeperGatewayMaster.init()
    val slaveTask  = bookkeeperGatewaySlave.init()

    val contextForClosingLedgers =
      Executors.newSingleThreadScheduledExecutor()

    val taskCloseLedgers = contextForClosingLedgers.scheduleWithFixedDelay(
      bookkeeperGatewayMaster,
      0,
      createNewLedgerEveryTimeMs/5,
      TimeUnit.MILLISECONDS
    )

    Thread.sleep(createNewLedgerEveryTimeMs)

    val rand = scala.util.Random
    val stringLength = 10

    val dataNumber = 100
    val data = new Array[String](dataNumber)

    bookkeeperGatewayMaster.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 0
      data.zipWithIndex.foreach { case (_, index) =>
        val str = rand.nextString(stringLength)
        data(index) = str
        currentLedger.addEntry(str.getBytes())
      }
    }

    Thread.sleep(createNewLedgerEveryTimeMs)

    val closedMasterLedgers = bookkeeperGatewayMaster.getClosedLedgers.asScala
    val closedSlaveLedgers = bookkeeperGatewaySlave.getClosedLedgers.asScala
    closedSlaveLedgers.size shouldBe 1

    val masterLedgerWithData = closedMasterLedgers.head
    val otherMasterLedgers   = closedMasterLedgers.tail

    val lastRecordMasterConfirmed = masterLedgerWithData.readLastConfirmed()
    lastRecordMasterConfirmed shouldBe dataNumber -1

    val enumMaster = masterLedgerWithData.readEntries(0L, lastRecordMasterConfirmed)
    data foreach {datum =>
      val ledgerEntry = enumMaster.nextElement()
      datum shouldBe new String(ledgerEntry.getEntry)
    }
    otherMasterLedgers.foreach(ledger => ledger.getLastAddConfirmed shouldBe -1L)


    val slaveLedgerWithData = closedSlaveLedgers.head

    val lastRecordSlaveConfirmed = slaveLedgerWithData.readLastConfirmed()
    lastRecordSlaveConfirmed shouldBe dataNumber -1
    val enumSlave = slaveLedgerWithData.readEntries(0L, lastRecordSlaveConfirmed)
    data foreach {datum =>
      val ledgerEntry = enumSlave.nextElement()
      datum shouldBe new String(ledgerEntry.getEntry)
    }


    masterTask.cancel(true)
    slaveTask.cancel(true)

    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }

  it should "transit its role to 'slave' and handle it correctly" in {
    val (zkServer, zkClient, bookies) =
      Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val selector1 = new LeaderSelector(zkClient, electionPath)
    val selector2 = new LeaderSelector(zkClient, electionPath)

    val bookkeeperGateway1 = new BookkeeperGateway(
      zkClient,
      selector1,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val bookkeeperGateway2 = new BookkeeperGateway(
      zkClient,
      selector2,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val task1 = bookkeeperGateway1.init()
    val task2 = bookkeeperGateway2.init()


    val contextForClosingLedgers1 =
      Executors.newSingleThreadScheduledExecutor()

    val taskCloseLedgers1 = contextForClosingLedgers1.scheduleWithFixedDelay(
      bookkeeperGateway1,
      0,
      createNewLedgerEveryTimeMs/5,
      TimeUnit.MILLISECONDS
    )

    val contextForClosingLedgers2 =
      Executors.newSingleThreadScheduledExecutor()

    val taskCloseLedgers2 = contextForClosingLedgers2.scheduleWithFixedDelay(
      bookkeeperGateway2,
      0,
      createNewLedgerEveryTimeMs/5,
      TimeUnit.MILLISECONDS
    )

    Thread.sleep(createNewLedgerEveryTimeMs*3)
    val closedLedgers1 =
      bookkeeperGateway1.getClosedLedgers
        .asScala.toArray.map(_.getId)

    task1.cancel(true)
    bookkeeperGateway1.close()
    taskCloseLedgers1.cancel(true)
    contextForClosingLedgers1.shutdown()


    Thread.sleep(createNewLedgerEveryTimeMs*5)
    val closedLedgers2 =
      bookkeeperGateway2.getClosedLedgers
        .asScala.toArray.map(_.getId)

    val newLedgers2 = closedLedgers2.diff(closedLedgers1)
    newLedgers2 shouldBe sorted

    task2.cancel(true)
    bookkeeperGateway2.close()
    taskCloseLedgers2.cancel(true)
    contextForClosingLedgers2.shutdown()

    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }
}
