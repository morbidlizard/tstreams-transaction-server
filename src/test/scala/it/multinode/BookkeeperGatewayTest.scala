package it.multinode

import java.io.File

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookKeeperGateway, Electable, ReplicationConfig}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import util.Utils


class BookkeeperGatewayTest
  extends FlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
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

  private val masterSelector = new Electable {
    override def hasLeadership: Boolean = true
    override def stopParticipateInElection(): Unit = {}
  }

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber

  private val bkLedgerPassword =
    "test".getBytes()

  private val createNewLedgerEveryTimeMs =
    250

  private lazy val serverExecutionContext =
    new ServerExecutionContextGrids(2, 2)
  private val authOptions =
    com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions()
  private val storageOptions =
    StorageOptions()
  private val rocksStorageOptions =
    RocksStorageOptions()

  private def uuid = java.util.UUID.randomUUID.toString
  private def startTransactionServer(zkClient: CuratorFramework): TransactionServer = {
    val path = s"/tts/$uuid"
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)

    new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
  }

  val (zkServer, zkClient, bookies) =
    Utils.startZkServerBookieServerZkClient(bookiesNumber)

  override def afterAll(): Unit = {
    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
  }

  override def afterEach(): Unit =
    beforeEach()



  "Bookkeeper gateway" should "return ledger the first created ledger." in {
    val transactionServer =
      startTransactionServer(zkClient)

    val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val trees = Array(zkTree1, zkTree2)


    val bookKeeperGateway = new BookKeeperGateway(
      transactionServer,
      zkClient,
      masterSelector,
      trees,
      replicationConfig,
      bkLedgerPassword,
      createNewLedgerEveryTimeMs
    )

    bookKeeperGateway.init()

    Thread.sleep(createNewLedgerEveryTimeMs)

    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 0
    }

    bookKeeperGateway.shutdown()
    transactionServer
      .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
  }

  it should "return the second created ledger for write operations as first is closed " +
    "and the first should be ready for retrieving data." in {
    val transactionServer =
      startTransactionServer(zkClient)

    val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val trees = Array(zkTree1, zkTree2)


    val bookKeeperGateway = new BookKeeperGateway(
      transactionServer,
      zkClient,
      masterSelector,
      trees,
      replicationConfig,
      bkLedgerPassword,
      createNewLedgerEveryTimeMs
    )


    bookKeeperGateway.init()
    Thread.sleep(createNewLedgerEveryTimeMs*2)

    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
      currentLedger.getId shouldBe 3
    }

    bookKeeperGateway.shutdown()
    transactionServer
      .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
  }
}
