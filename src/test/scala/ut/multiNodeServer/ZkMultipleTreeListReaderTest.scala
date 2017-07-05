package ut.multiNodeServer

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.RecordType
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.{BookKeeperWrapper, ReplicationConfig}
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{StorageManager, ZkMultipleTreeListReader}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ut.multiNodeServer.ZkTreeListTest.StorageManagerInMemory
import util.Utils

class ZkMultipleTreeListReaderTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {
  private val rand = scala.util.Random
  private val streamIDGen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val partitionsNumber = 100

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

  private val passwordBookKeeper =
    "test".getBytes()

  private lazy val (zkServer, zkClient, bookies) =
    Utils.startZkServerBookieServerZkClient(bookiesNumber)

  private lazy val bookKeeper: BookKeeper = {
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


  private def generateStream =
    Stream(
      id = streamIDGen.getAndIncrement(),
      name = rand.nextString(10),
      partitions = partitionsNumber,
      None,
      Long.MaxValue,
      ""
    )

  private def getRandomProducerTransaction(streamID: Int,
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

  private def uuid = java.util.UUID.randomUUID.toString


  override def afterAll(): Unit = {
    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  "ZkMultipleTreeListReader" should "not retrieve records from database ZkTreeListLong objects don't have entities" in {
    val storage = new StorageManagerInMemory

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val ledgerRecordIDs = Array.empty[LedgerIDAndItsLastRecordID]
    val (records, updatedLedgersWithTheirLastRecords) =
      testReader.process(ledgerRecordIDs)

    records shouldBe empty
    ledgerRecordIDs should contain theSameElementsAs updatedLedgersWithTheirLastRecords
  }


  it should "not retrieve records as one of ZkTreeListLong objects doesn't have entities" in {
    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList2.createNode(firstLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val ledgerRecordIDs = Array.empty[LedgerIDAndItsLastRecordID]
    val (records, updatedLedgersWithTheirLastRecords) =
      testReader.process(ledgerRecordIDs)

    records shouldBe empty
    ledgerRecordIDs should contain theSameElementsAs updatedLedgersWithTheirLastRecords
  }


  private def test1(storage: StorageManager) = {
    val stream = generateStream

    val producerTransactionsNumber = 50

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(initialTime)

    val secondTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val firstLedger = storage.createLedger()
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)
    firstLedger.close()

    val secondLedger = storage.createLedger()
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)
    secondLedger.close()

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.process(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe producerTransactionsNumber * 2 + 2
    records2 shouldBe empty

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = firstLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " ledgers are closed at the same time" in {
    val bookKeeperStorage = new BookKeeperWrapper(
      bookKeeper,
      replicationConfig,
      passwordBookKeeper
    )

    val storage = new StorageManagerInMemory

    test1(storage)
    test1(bookKeeperStorage)
  }


  private def test2(storage: StorageManager) = {
    val stream = generateStream

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(initialTime + 50)

    val secondTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val firstLedger = storage.createLedger()
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)
    firstLedger.close()

    val secondLedger = storage.createLedger()
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)
    secondLedger.close()

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.process(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 150
    records2.length shouldBe 0

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = firstLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = 49
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }
  it should "retrieve records from database ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " first ledger(belongs to 'treeList1') is closed earlier than second ledger(belongs to 'treeList2')" in {
    val bookKeeperStorage = new BookKeeperWrapper(
      bookKeeper,
      replicationConfig,
      passwordBookKeeper
    )

    val storage = new StorageManagerInMemory

    test2(storage)
    test2(bookKeeperStorage)
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " the first ledger(belongs to 'treeList1') is closed earlier than the second ledger(belongs to 'treeList2), and the third ledger(belongs to 'treeList1)" in {
    val stream = generateStream

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstLedgerRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val firstBarrier = atomicLong.getAndSet(initialTime + 50)

    val secondLedgerRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(firstBarrier + 20)

    val thirdLedgerRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val thirdTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )


    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()
    firstLedgerRecords.foreach(record => firstLedger.addRecord(record))
    firstLedger.addRecord(firstTimestampRecord)
    firstLedger.close()

    val secondLedger = storage.createLedger()
    secondLedgerRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)
    secondLedger.close()

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val thirdLedger = storage.createLedger()
    thirdLedgerRecords.foreach(record => thirdLedger.addRecord(record))
    thirdLedger.addRecord(thirdTimestampRecord)
    thirdLedger.close()

    zkTreeList1.createNode(thirdLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.process(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 150
    records2.length shouldBe 80

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = firstLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = 49
      )

    updatedLedgersWithTheirLastRecords2.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = thirdLedger.id,
        ledgerLastRecordID = 29
      )

    updatedLedgersWithTheirLastRecords2.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )
  }

  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " second ledger(belongs to 'treeList2') is closed earlier than first ledger(belongs to 'treeList1')" in {
    val stream = generateStream

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Opened,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    atomicLong.set(initialTime - 50)

    val secondTreeRecords = {
      (0 until producerTransactionsNumber)
        .map(txnID => getRandomProducerTransaction(
          stream.id,
          1,
          Checkpointed,
          txnID,
          50000L
        ))
        .map { txn =>
          val binaryTransaction = Protocol.PutTransaction.encodeRequest(
            TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
          )
          new Record(
            RecordType.PutTransactionType,
            atomicLong.getAndIncrement(),
            binaryTransaction
          )
        }
    }

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

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.process(updatedLedgersWithTheirLastRecords1)

    records1.length shouldBe 150
    records2.length shouldBe 0

    updatedLedgersWithTheirLastRecords1.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = firstLedger.id,
        ledgerLastRecordID = 49
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
  }

  it should "process ledgers records that created earlier than the last one" in {
    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTimestampRecord = new TimestampRecord(
      atomicLong.getAndSet(150L)
    )

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndSet(300L)
    )

    val thirdTimestampRecord = new TimestampRecord(
      atomicLong.getAndSet(350L)
    )

    val forthTimestampRecord = new TimestampRecord(
      atomicLong.getAndSet(400L)
    )

    val storage = new StorageManagerInMemory

    val firstLedger = storage.createLedger()
    firstLedger.addRecord(firstTimestampRecord)
    firstLedger.close()

    val secondLedger = storage.createLedger()
    secondLedger.addRecord(secondTimestampRecord)
    secondLedger.close()

    val thirdLedger = storage.createLedger()
    thirdLedger.addRecord(thirdTimestampRecord)
    thirdLedger.close()

    val forthLedger = storage.createLedger()
    forthLedger.addRecord(forthTimestampRecord)
    forthLedger.close()


    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")


    zkTreeList2.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)
    zkTreeList2.createNode(thirdLedger.id)

    zkTreeList1.createNode(forthLedger.id)

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val (records1, updatedLedgersWithTheirLastRecords1) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    records1.length shouldBe 1
    updatedLedgersWithTheirLastRecords1.head.ledgerID shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords1.head.ledgerLastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords1.tail.head.ledgerID shouldBe firstLedger.id
    updatedLedgersWithTheirLastRecords1.tail.head.ledgerLastRecordID shouldBe 0L

    val (records2, updatedLedgersWithTheirLastRecords2) =
      testReader.process(updatedLedgersWithTheirLastRecords1)

    records2.length shouldBe 1
    updatedLedgersWithTheirLastRecords2.head.ledgerID shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords2.head.ledgerLastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords2.tail.head.ledgerID shouldBe secondLedger.id
    updatedLedgersWithTheirLastRecords2.tail.head.ledgerLastRecordID shouldBe 0L

    val (records3, updatedLedgersWithTheirLastRecords3) =
      testReader.process(updatedLedgersWithTheirLastRecords2)

    records3.length shouldBe 1
    updatedLedgersWithTheirLastRecords3.head.ledgerID shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords3.head.ledgerLastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords3.tail.head.ledgerID shouldBe thirdLedger.id
    updatedLedgersWithTheirLastRecords3.tail.head.ledgerLastRecordID shouldBe 0L

    val (records4, updatedLedgersWithTheirLastRecords4) =
      testReader.process(updatedLedgersWithTheirLastRecords3)

    records4 shouldBe empty
    updatedLedgersWithTheirLastRecords4.head.ledgerID shouldBe forthLedger.id
    updatedLedgersWithTheirLastRecords4.head.ledgerLastRecordID shouldBe -1L
    updatedLedgersWithTheirLastRecords4.tail.head.ledgerID shouldBe thirdLedger.id
    updatedLedgersWithTheirLastRecords4.tail.head.ledgerLastRecordID shouldBe 0L
  }

}
