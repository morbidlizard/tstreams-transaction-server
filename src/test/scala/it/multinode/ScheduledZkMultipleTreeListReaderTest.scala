package it.multinode

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{ScheduledZkMultipleTreeListReader, ZkMultipleTreeListReader, ZookeeperTreeListLong}
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.{RecordType, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import ut.multiNodeServer.ZkTreeListTest.StorageManagerInMemory
import util.Utils

import scala.collection.mutable

class ScheduledZkMultipleTreeListReaderTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  private def uuid = java.util.UUID.randomUUID.toString

  private lazy val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
  private val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthenticationOptions()
  private val storageOptions = StorageOptions()
  private val rocksStorageOptions = RocksStorageOptions()

  private def startTransactionServer(zkClient: CuratorFramework): TransactionServer = {
    val path = s"/tts/$uuid"
    val streamDatabaseZK = new ZookeeperStreamRepository(zkClient, path)

    new TransactionServer(
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
  }


  private val rand = scala.util.Random

  private val streamIDGen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val partitionsNumber = 100

  private def generateStream =
    Stream(
      id = streamIDGen.getAndIncrement(),
      name = rand.nextString(10),
      partitions = partitionsNumber,
      None,
      Long.MaxValue,
      ""
    )

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

  private def buildConsumerTransaction(streamID: Int,
                                       partition: Int,
                                       txnID: Long,
                                       name: String) =
    ConsumerTransaction(
      streamID,
      partition,
      txnID,
      name
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
          RecordType.PutTransactionType,
          transactionIDGen.getAndIncrement(),
          binaryTransaction
        )
      }
  }

  private def genConsumerTransactionsWrappedInRecords(consumerMap: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                                      transactionIDGen: AtomicLong,
                                                      transactionNumber: Int,
                                                      streamID: Int,
                                                      partition: Int,
                                                      name: String) = {
    (0 until transactionNumber)
      .map { txnID =>
        val consumerTransaction = buildConsumerTransaction(
          streamID,
          partition,
          txnID,
          name)

        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(Transaction(None, Some(consumerTransaction)))
        )

        val record = new Record(
          RecordType.PutTransactionType,
          transactionIDGen.getAndIncrement(),
          binaryTransaction
        )

        val binaryTransactionRecord =
          ConsumerTransactionRecord(
            consumerTransaction,
            record.timestamp
          )

        val update = consumerMap.get(binaryTransactionRecord.key)
          .map(txn =>
            if (txn.timestamp < binaryTransactionRecord.timestamp)
              binaryTransactionRecord
            else
              txn
          )
          .getOrElse(binaryTransactionRecord)

        consumerMap.put(update.key, update)

        record
      }
  }

  private lazy val (zkServer, zkClient) = Utils.startZkServerAndGetIt

  override def afterAll(): Unit = {
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


  it should "return opened and checkpointed transactions and process entirely 2 ledgers as they are opened at the same time and closed too" in {
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Opened,
        50000L
      )

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(initialTime)

    val secondTreeRecords =
       genProducerTransactionsWrappedInRecords(
         atomicLong,
         producerTransactionsNumber,
         stream.id,
         partition,
         Checkpointed,
         50000L
      )

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)

    val secondLedger = storage.createLedger()
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )
    val transactionServer = startTransactionServer(zkClient)

    val scheduledZkMultipleTreeListReader =
      new ScheduledZkMultipleTreeListReader(
        testReader,
        transactionServer
      )

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val result = transactionServer.scanTransactions(
      stream.id,
      partition,
      initialTime,
      atomicLong.get(),
      Int.MaxValue,
      Set(TransactionStates.Opened)
    )

    val processedLedgerAndRecord1 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord1 shouldBe defined

    result.producerTransactions.length shouldBe producerTransactionsNumber
    result.producerTransactions.forall(_.state == TransactionStates.Checkpointed) shouldBe true
    result.producerTransactions.last.transactionID shouldBe producerTransactionsNumber - 1L

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val processedLedgerAndRecord2 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord2 shouldBe defined

    for {
      first <- processedLedgerAndRecord1
      second <- processedLedgerAndRecord2
    } yield first should contain theSameElementsInOrderAs second


    transactionServer.closeAllDatabases()
  }


  it should "return checkpointed transactions and process entirely 1-st ledger records and half of records of 2-nd ledgers" in {
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Opened,
        50000L
      )

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val offset = 50
    atomicLong.set(initialTime + offset)


    val secondTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Checkpointed,
        50000L
      )

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)

    val secondLedger = storage.createLedger()
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )
    val transactionServer = startTransactionServer(zkClient)

    val scheduledZkMultipleTreeListReader =
      new ScheduledZkMultipleTreeListReader(
        testReader,
        transactionServer
      )

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val result = transactionServer.scanTransactions(
      stream.id,
      partition,
      initialTime,
      atomicLong.get(),
      Int.MaxValue,
      Set()
    )

    val processedLedgerAndRecord1 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord1 shouldBe defined

    result.producerTransactions.length shouldBe producerTransactionsNumber
    result.producerTransactions.take(offset).forall(_.state == TransactionStates.Checkpointed) shouldBe true
    result.producerTransactions.takeRight(offset-1).forall(_.state == TransactionStates.Opened)  shouldBe true
    result.producerTransactions.last.transactionID shouldBe producerTransactionsNumber - 1L

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val processedLedgerAndRecord2 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord2 shouldBe defined

    for {
      first <- processedLedgerAndRecord1
      second <- processedLedgerAndRecord2
    } yield first should contain theSameElementsInOrderAs second


    transactionServer.closeAllDatabases()
  }


  it should "return consumer transactions properly" in {
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)


    val streamsNames =
      Array.fill(producerTransactionsNumber)(uuid)

    val consumerTransactionRecords =
      mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord]()

    val firstTreeRecords =
      genConsumerTransactionsWrappedInRecords(
        consumerTransactionRecords,
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        streamsNames(rand.nextInt(producerTransactionsNumber))
      )

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(initialTime)


    val secondTreeRecords =
      genConsumerTransactionsWrappedInRecords(
        consumerTransactionRecords,
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        streamsNames(rand.nextInt(producerTransactionsNumber))
      )

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)

    val secondLedger = storage.createLedger()
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )
    val transactionServer = startTransactionServer(zkClient)

    val scheduledZkMultipleTreeListReader =
      new ScheduledZkMultipleTreeListReader(
        testReader,
        transactionServer
      )

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val processedLedgerAndRecord1 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord1 shouldBe defined

    consumerTransactionRecords.foreach {
      case (consumerKey, consumerValue) =>
        transactionServer.getConsumerState(
          consumerKey.name,
          consumerKey.streamID,
          consumerKey.partition
        ) shouldBe consumerValue.transactionID
    }

    scheduledZkMultipleTreeListReader.processAndPersistRecords()

    val processedLedgerAndRecord2 = transactionServer.getLastProcessedLedgersAndRecordIDs
    processedLedgerAndRecord2 shouldBe defined

    for {
      first <- processedLedgerAndRecord1
      second <- processedLedgerAndRecord2
    } yield first should contain theSameElementsInOrderAs second


    transactionServer.closeAllDatabases()
  }
}
