package ut.multiNodeServer

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.RecordType
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.ZkMultipleTreeListReader
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ut.multiNodeServer.ZkTreeListTest.StorageManagerInMemory
import util.Utils

class ZkMultipleTreeListReaderTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
{
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

  private def getRandomProducerTransaction(streamID:Int,
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

  private lazy val (zkServer, zkClient) = Utils.startZkServerAndGetIt
  override def afterAll(): Unit = {
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
    ledgerRecordIDs should contain theSameElementsAs ledgerRecordIDs
  }


  it should "retrieve records from database because ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " ledgers are closed at the same time" in {
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

  it should "retrieve records from database ZkTreeListLong objects called 'treeList1' and 'treeList2' have ledgers ids and a storage contains records within the ledgers," +
    " first ledger(belongs to 'treeList1') is closed earlier than second ledger(belongs to 'treeList2')" in {
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
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords1.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = 49
      )


    updatedLedgersWithTheirLastRecords1 should contain theSameElementsInOrderAs updatedLedgersWithTheirLastRecords2
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

    val secondLedger = storage.createLedger()
    secondLedgerRecords.foreach(record => secondLedger.addRecord(record))
    secondLedger.addRecord(secondTimestampRecord)

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val thirdLedger = storage.createLedger()
    thirdLedgerRecords.foreach(record => thirdLedger.addRecord(record))
    thirdLedger.addRecord(thirdTimestampRecord)

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

}
