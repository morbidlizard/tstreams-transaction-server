package it.multinode


import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService._
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils


class BookkeeperMasterTest
  extends FlatSpec
    with BeforeAndAfterAll
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

  private val masterSelector = new LeaderSelectorInterface {
    override def hasLeadership: Boolean = true
    override def stopParticipateInElection(): Unit = {}
  }

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber

  private val bkLedgerPassword =
    "test".getBytes()

  private val createNewLedgerEveryTimeMs =
    250

  private def uuid = java.util.UUID.randomUUID.toString

  private def buildProducerTransaction(streamID: Int,
                                       partition: Int,
                                       state: TransactionStates,
                                       txnID: Long,
                                       ttlTxn: Long) =
    ProducerTransaction(
      stream = streamID,
      partition = partition,
      transactionID = txnID,
      state = state,
      quantity = -1,
      ttl = ttlTxn
    )


  private def genProducerTransactionsWrappedInRecords(transactionIDGen: AtomicLong,
                                                      transactionNumber: Int,
                                                      streamID: Int,
                                                      partition: Int,
                                                      state: TransactionStates,
                                                      ttlTxn: Long) = {
    (0 until transactionNumber)
      .map(txnID => buildProducerTransaction(
        streamID,
        partition,
        state,
        txnID,
        ttlTxn
      ))
      .map { txn =>
        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
        )
        new Record(
          Frame.PutTransactionType,
          transactionIDGen.getAndIncrement(),
          binaryTransaction
        )
      }
  }

  private lazy val (zkServer, zkClient, bookies) =
    Utils.startZkServerBookieServerZkClient(bookiesNumber)

  private lazy val bookkeeper: BookKeeper = {
    val lowLevelZkClient = zkClient.getZookeeperClient
    val configuration = new ClientConfiguration()
      .setZkServers(
        lowLevelZkClient.getCurrentConnectionString
      )
      .setZkTimeout(lowLevelZkClient.getConnectionTimeoutMs)

    configuration.setLedgerManagerFactoryClass(
      classOf[HierarchicalLedgerManagerFactory]
    )

    new BookKeeper(configuration)
  }

  override def beforeAll(): Unit = {
    zkServer
    zkClient
    bookies
  }

  override def afterAll(): Unit = {
    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  "Bookkeeper master" should "return the first created ledger." in {
    val bundle = util.multiNode
      .Util.getTransactionServerBundle(zkClient)

    bundle.operate { _ =>
      val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")


      val bookkeeperMaster =
        new BookkeeperMaster(
          bookkeeper,
          masterSelector,
          replicationConfig,
          zkTree1,
          bkLedgerPassword,
          createNewLedgerEveryTimeMs
        )


      val bookkeeperMasterBundle =
        new BookkeeperMasterBundle(
          bookkeeperMaster,
          createNewLedgerEveryTimeMs
        )

      bookkeeperMasterBundle.start()
      Thread.sleep(createNewLedgerEveryTimeMs)

      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedger =>
        currentLedger.isRight shouldBe true
        currentLedger.right.get.getId shouldBe 0
      }

      bookkeeperMasterBundle.stop()
    }
  }

  it should "return new ledger for write operations as previous is closed" in {
    val bundle = util.multiNode
      .Util.getTransactionServerBundle(zkClient)

    bundle.operate { _ =>

      val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

      val bookkeeperMaster =
        new BookkeeperMaster(
          bookkeeper,
          masterSelector,
          replicationConfig,
          zkTree1,
          bkLedgerPassword,
          createNewLedgerEveryTimeMs
        )

      val bookkeeperMasterBundle =
        new BookkeeperMasterBundle(
          bookkeeperMaster,
          createNewLedgerEveryTimeMs
        )

      bookkeeperMasterBundle.start()
      Thread.sleep(createNewLedgerEveryTimeMs * 3)


      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedger =>
        currentLedger.isRight shouldBe true
        currentLedger.right.get.getId should be > 1L
      }

      bookkeeperMasterBundle.stop()
    }
  }

  it should "create ledger, put producer records and through the while read them" in {
    val bundle = util.multiNode
      .Util.getTransactionServerBundle(zkClient)

    bundle.operate { transactionServer =>
      val partition = 1
      val transactionNumber = 30

      val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
      val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
      val zkTrees = Array(zkTree1, zkTree2)

      val bookkeeperMaster =
        new BookkeeperMaster(
          bookkeeper,
          masterSelector,
          replicationConfig,
          zkTree1,
          bkLedgerPassword,
          createNewLedgerEveryTimeMs
        )

      val bookkeeperMasterBundle =
        new BookkeeperMasterBundle(
          bookkeeperMaster,
          createNewLedgerEveryTimeMs
        )

      val bookkeeperSlave =
        new BookkeeperSlave(
          bookkeeper,
          replicationConfig,
          zkTrees,
          bundle.multiNodeCommitLogService,
          bundle.rocksWriter,
          bkLedgerPassword
        )

      val bookkeeperSlaveBundle =
        new BookkeeperSlaveBundle(
          bookkeeperSlave,
          createNewLedgerEveryTimeMs
        )

      bookkeeperMasterBundle.start()

      val initialTxnID = 0L
      val transactionIDGen = new AtomicLong(initialTxnID)

      val streamID =
        transactionServer.putStream(uuid, 100, None, 1000L)

      bookkeeperSlaveBundle.start()

      var currentLedgerOuterRef1: Long = -1L
      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedgerOrError =>

        val currentLedger = currentLedgerOrError.right.get
        transactionIDGen.set(System.currentTimeMillis())
        currentLedgerOuterRef1 = currentLedger.getId
        val records = genProducerTransactionsWrappedInRecords(
          transactionIDGen,
          transactionNumber,
          streamID,
          partition,
          TransactionStates.Opened,
          10000L
        )
        records.foreach(record => currentLedger.addEntry(record.toByteArray))
      }

      Thread.sleep(createNewLedgerEveryTimeMs * 2)

      var currentLedgerOuterRef2: Long = -1L
      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedger =>
        currentLedgerOuterRef2 = currentLedger.right.get.getId
      }

      zkTree2.createNode(currentLedgerOuterRef2)

      Thread.sleep(createNewLedgerEveryTimeMs)
      val result = transactionServer.scanTransactions(
        streamID,
        partition,
        initialTxnID,
        transactionIDGen.incrementAndGet(),
        transactionNumber,
        Set()
      )

      val producerTransactions =
        result.producerTransactions

      bookkeeperMasterBundle.stop()
      bookkeeperSlaveBundle.stop()

      producerTransactions.length shouldBe transactionNumber
      producerTransactions.foreach(_.state shouldBe TransactionStates.Opened)
    }
  }
}
