//package it.multinode
//
//import java.io.File
//import java.util.concurrent.{CountDownLatch, TimeUnit}
//import java.util.concurrent.atomic.AtomicLong
//
//import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
//import com.bwsw.tstreamstransactionserver.netty.Protocol
//import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
//import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
//import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
//import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.ZookeeperTreeListLong
//import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookKeeperGateway, Electable, ReplicationConfig}
//import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
//import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService, TransactionStates}
//import org.apache.commons.io.FileUtils
//import org.apache.curator.framework.CuratorFramework
//import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
//import util.Utils
//
//
//class BookkeeperGatewayTest
//  extends FlatSpec
//    with BeforeAndAfterAll
//    with BeforeAndAfterEach
//    with Matchers
//{
//
//  private val ensembleNumber = 4
//  private val writeQourumNumber = 3
//  private val ackQuorumNumber = 2
//
//  private val replicationConfig = ReplicationConfig(
//    ensembleNumber,
//    writeQourumNumber,
//    ackQuorumNumber
//  )
//
//  private val masterSelector = new Electable {
//    override def hasLeadership: Boolean = true
//    override def stopParticipateInElection(): Unit = {}
//  }
//
//  private val bookiesNumber =
//    ensembleNumber max writeQourumNumber max ackQuorumNumber
//
//  private val bkLedgerPassword =
//    "test".getBytes()
//
//  private val createNewLedgerEveryTimeMs =
//    250
//
//  private lazy val serverExecutionContext =
//    new ServerExecutionContextGrids(2, 2)
//  private val authOptions =
//    com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions()
//  private val storageOptions =
//    StorageOptions()
//  private val rocksStorageOptions =
//    RocksStorageOptions()
//
//  private def uuid = java.util.UUID.randomUUID.toString
//  private def startTransactionServer(zkClient: CuratorFramework): TransactionServer = {
//    val path = s"/tts/$uuid"
//    val streamDatabaseZK = new ZookeeperStreamRepository(zkClient, path)
//
//    new TransactionServer(
//      authOpts = authOptions,
//      storageOpts = storageOptions,
//      rocksStorageOpts = rocksStorageOptions,
//      streamDatabaseZK
//    )
//  }
//
//  private def buildProducerTransaction(streamID: Int,
//                                       partition: Int,
//                                       state: TransactionStates,
//                                       txnID: Long,
//                                       ttlTxn: Long) =
//    ProducerTransaction(
//      stream = streamID,
//      partition = partition,
//      transactionID = txnID,
//      state = state,
//      quantity = -1,
//      ttl = ttlTxn
//    )
//
//
//  private def genProducerTransactionsWrappedInRecords(transactionIDGen: AtomicLong,
//                                                      transactionNumber: Int,
//                                                      streamID: Int,
//                                                      partition: Int,
//                                                      state: TransactionStates,
//                                                      ttlTxn: Long) = {
//    (0 until transactionNumber)
//      .map(txnID => buildProducerTransaction(
//        streamID,
//        partition,
//        state,
//        txnID,
//        ttlTxn
//      ))
//      .map { txn =>
//        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
//          TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
//        )
//        new Record(
//          RecordType.PutTransactionType,
//          transactionIDGen.getAndIncrement(),
//          binaryTransaction
//        )
//      }
//  }
//
//  val (zkServer, zkClient, bookies) =
//    Utils.startZkServerBookieServerZkClient(bookiesNumber)
//
//  override def afterAll(): Unit = {
//    bookies.foreach(_.shutdown())
//    zkClient.close()
//    zkServer.close()
//  }
//
//  override def beforeEach(): Unit = {
//    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
//    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
//    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
//    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
//  }
//
//  override def afterEach(): Unit =
//    beforeEach()
//
//
//
//  "Bookkeeper gateway" should "return the first created ledger." in {
//    val transactionServer =
//      startTransactionServer(zkClient)
//
//    val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val trees = Array(zkTree1, zkTree2)
//
//
//    val bookKeeperGateway = new BookKeeperGateway(
//      transactionServer,
//      zkClient,
//      masterSelector,
//      trees,
//      replicationConfig,
//      bkLedgerPassword,
//      createNewLedgerEveryTimeMs
//    )
//
//    bookKeeperGateway.init()
//
//    Thread.sleep(createNewLedgerEveryTimeMs)
//
//    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
//      currentLedger.getId shouldBe 0
//    }
//
//    bookKeeperGateway.shutdown()
//    transactionServer
//      .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
//  }
//
//  it should "return the second created ledger for write operations as first is closed" in {
//    val transactionServer =
//      startTransactionServer(zkClient)
//
//    val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val trees = Array(zkTree1, zkTree2)
//
//
//    val bookKeeperGateway = new BookKeeperGateway(
//      transactionServer,
//      zkClient,
//      masterSelector,
//      trees,
//      replicationConfig,
//      bkLedgerPassword,
//      createNewLedgerEveryTimeMs
//    )
//
//
//    bookKeeperGateway.init()
//    Thread.sleep(createNewLedgerEveryTimeMs*2)
//
//    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
//      assert(currentLedger.getId > 1)
//    }
//
//    bookKeeperGateway.shutdown()
//    transactionServer
//      .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
//  }
//
//  it should "create ledger, put producer records and through the while read them" in {
//    val transactionServer =
//      startTransactionServer(zkClient)
//
//    val zkTree1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val zkTree2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
//    val trees = Array(zkTree1, zkTree2)
//
//
//    val bookKeeperGateway = new BookKeeperGateway(
//      transactionServer,
//      zkClient,
//      masterSelector,
//      trees,
//      replicationConfig,
//      bkLedgerPassword,
//      createNewLedgerEveryTimeMs
//    )
//
//    val initialTxnID = 0L
//    val transactionIDGen = new AtomicLong(initialTxnID)
//    val streamID = transactionServer.putStream(uuid, 100, None, 1000L)
//    val partition = 1
//    val transactionNumber = 30
//
//    bookKeeperGateway.init()
//
//    var currentLedgerOuterRef1: Long = -1L
//    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
//      transactionIDGen.set(System.currentTimeMillis())
//      currentLedgerOuterRef1 = currentLedger.getId
//      val records = genProducerTransactionsWrappedInRecords(
//        transactionIDGen,
//        transactionNumber,
//        streamID,
//        partition,
//        TransactionStates.Opened,
//        10000L
//      )
//      records.foreach(record => currentLedger.addEntry(record.toByteArray))
//    }
//
//    Thread.sleep(createNewLedgerEveryTimeMs*2)
//
//    var currentLedgerOuterRef2: Long = -1L
//    bookKeeperGateway.doOperationWithCurrentWriteLedger { currentLedger =>
//      currentLedgerOuterRef2 = currentLedger.getId
//    }
//
//    zkTree2.createNode(currentLedgerOuterRef2)
//
//    val latch = new CountDownLatch(transactionNumber)
//    (0 until transactionNumber).foreach { id =>
//      transactionServer.notifyProducerTransactionCompleted(
//        txn => {
//          txn.transactionID == id && txn.state == TransactionStates.Opened
//        }, {
//          latch.countDown()
//        }
//      )
//    }
//
//    latch.await(5000, TimeUnit.MILLISECONDS) shouldBe true
//
//    bookKeeperGateway.shutdown()
//    transactionServer
//      .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
//  }
//}
