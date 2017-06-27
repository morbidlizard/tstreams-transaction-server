package ut.multiNodeServer

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.ZkMultipleTreeListReader
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType, TimestampRecord}
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


  "ZkMultipleTreeListReaderTest" should "not retrieve any records from database because ZkTreeListLong doesn't have entities" in {
    val storage = new StorageManagerInMemory

    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, s"/$uuid")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, s"/$uuid")

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      storage
    )

    val (records, updatedLedgersWithTheirLastRecords) =
      testReader.process(Array.empty[LedgerIDAndItsLastRecordID])

    records shouldBe empty
    updatedLedgersWithTheirLastRecords.length shouldBe trees.length
    updatedLedgersWithTheirLastRecords.foreach(ledgerIDAndItsLastRecordID =>
      ledgerIDAndItsLastRecordID shouldBe
        LedgerIDAndItsLastRecordID(ledgerID = -1L, ledgerLastRecordID = -1L)
    )
  }


  it should "retrieve records from database because ZkTreeListLong have ledgers ids and a storage contains records within the ledgers," +
    "ledgers are closed at the same time" in {
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
            RecordType.Transaction,
            atomicLong.getAndIncrement(),
            binaryTransaction
          ).toByteArray
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
            RecordType.Transaction,
            atomicLong.getAndIncrement(),
            binaryTransaction
          ).toByteArray
        }
    }

    val secondTimestampRecord = new TimestampRecord(
      atomicLong.getAndIncrement()
    )

    val storage = new StorageManagerInMemory

    val firstLedger = storage.addLedger()
    firstTreeRecords.foreach(binaryRecord => firstLedger.addEntry(binaryRecord))
    firstLedger.addEntry(firstTimestampRecord.toByteArray)

    val secondLedger = storage.addLedger()
    secondTreeRecords.foreach(binaryRecord => secondLedger.addEntry(binaryRecord))
    secondLedger.addEntry(secondTimestampRecord.toByteArray)

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

    updatedLedgersWithTheirLastRecords2.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = firstLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )

    updatedLedgersWithTheirLastRecords2.tail.head shouldBe
      LedgerIDAndItsLastRecordID(ledgerID = secondLedger.id,
        ledgerLastRecordID = producerTransactionsNumber
      )
  }

}
