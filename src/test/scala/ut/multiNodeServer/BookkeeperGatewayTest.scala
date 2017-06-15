package ut.multiNodeServer

import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.{BookkeeperGateway, ReplicationConfig, ServerRole}
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

import scala.collection.JavaConverters._

class BookkeeperGatewayTest
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

  private val masterSelector = new ServerRole {
    override def hasLeadership: Boolean = true
  }

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber

  private val ledgerLogPath =
    "/tts/log1"

  private val passwordLedgerLogPath =
    "test".getBytes()

  private val createNewLedgerEveryTimeMs =
    250


  "Bookkeeper gateway" should "return ledger the first created ledger." in {
    val (zkServer, zkClient, bookies) =
      Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val bookkeeperGateway = new BookkeeperGateway(
      zkClient,
      masterSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val task = bookkeeperGateway.init()

    Thread.sleep(createNewLedgerEveryTimeMs)

    bookkeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 0
    }

    task.cancel(true)
    zkClient.close()
    bookies.foreach(_.shutdown())
    zkServer.close()
  }

  it should "return the second created ledger for write operations as first is closed " +
    "and the first should be ready for retrieving data." in {
    val (zkServer, zkClient, bookies) =
      Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val bookkeeperGateway = new BookkeeperGateway(
      zkClient,
      masterSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val task = bookkeeperGateway.init()
    val contextForClosingLedgers =
      Executors.newSingleThreadScheduledExecutor()

    val taskCloseLedgers = contextForClosingLedgers.scheduleWithFixedDelay(
      bookkeeperGateway,
      0,
      createNewLedgerEveryTimeMs/5,
      TimeUnit.MILLISECONDS
    )

    Thread.sleep(createNewLedgerEveryTimeMs*2)

    val closedLedgers = bookkeeperGateway.getClosedLedgers
    bookkeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 1
    }

    closedLedgers.size() shouldBe 1

    closedLedgers.forEach(ledger =>
      ledger.isClosed shouldBe true
    )

    taskCloseLedgers.cancel(true)
    task.cancel(true)

    zkClient.close()
    bookies.foreach(_.shutdown())
    zkServer.close()
  }

  it should "return the second created ledger for write operations as first is closed " +
    "and the first should contain data and be ready for retrieving data." in {
    val (zkServer, zkClient, bookies) =
      Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val bookkeeperGateway = new BookkeeperGateway(
      zkClient,
      masterSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val task = bookkeeperGateway.init()
    val contextForClosingLedgers =
      Executors.newSingleThreadScheduledExecutor()

    val taskCloseLedgers = contextForClosingLedgers.scheduleWithFixedDelay(
      bookkeeperGateway,
      0,
      createNewLedgerEveryTimeMs/5,
      TimeUnit.MILLISECONDS
    )

    Thread.sleep(createNewLedgerEveryTimeMs)

    val rand = scala.util.Random
    val stringLength = 10

    val dataNumber = 100
    val data = new Array[String](dataNumber)

    bookkeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 0
      data.zipWithIndex.foreach { case (_, index) =>
        val str = rand.nextString(stringLength)
        data(index) = str
        currentLedger.addEntry(str.getBytes())
      }
    }

    Thread.sleep(createNewLedgerEveryTimeMs)
    val closedLedgers = bookkeeperGateway.getClosedLedgers.asScala

    val ledgerWithData = closedLedgers.head
    val otherLedgers   = closedLedgers.tail

    val lastRecordConfirmed = ledgerWithData.readLastConfirmed()
    lastRecordConfirmed shouldBe dataNumber -1

    val enum = ledgerWithData.readEntries(0L, lastRecordConfirmed)
    data foreach {datum =>
      val ledgerEntry = enum.nextElement()
      datum shouldBe new String(ledgerEntry.getEntry)
    }

    otherLedgers.foreach(ledger => ledger.getLastAddConfirmed shouldBe -1L)

    taskCloseLedgers.cancel(true)
    task.cancel(true)

    zkClient.close()
    bookies.foreach(_.shutdown())
    zkServer.close()
  }


}
