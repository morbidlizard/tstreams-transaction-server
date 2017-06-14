package ut.multiNodeServer

import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.{BookkeeperGateway, ReplicationConfig, ServerRole}
import org.scalatest.{FlatSpec, Matchers}
import util.Utils

class BookkeeperGatewayTest
  extends FlatSpec
    with Matchers
{

  private val ensembleNumber = 5
  private val writeQourumNumber = 4
  private val ackQuorumNumber = 3

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

  private val createNewLedgerEveryTimeMs =
    500


  "Bookkeeper gateway" should "ad" in {
    val (zkServer, zkClient, bookies) = Utils.startZkServerBookieServerZkClient(bookiesNumber)

    val masterSelector = new ServerRole {
      override def hasLeadership: Boolean = true
    }

    val bookeeperGateway = new BookkeeperGateway(
      zkClient,
      masterSelector,
      replicationConfig,
      ledgerLogPath,
      passwordLedgerLogPath,
      createNewLedgerEveryTimeMs
    )

    val task = bookeeperGateway.init()

    Thread.sleep(createNewLedgerEveryTimeMs)
    println(bookeeperGateway.currentLedgerHandle.get.getId)




    task.cancel(true)
    zkClient.close()
    bookies.foreach(_.shutdown())
    zkServer.close()
  }

}
