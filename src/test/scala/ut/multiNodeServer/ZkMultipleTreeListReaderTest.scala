package ut.multiNodeServer

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.bookkeeperService.hierarchy.ZookeeperTreeListLong
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabase
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.ZkMultipleTreeListReader
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType, TimestampRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import ut.multiNodeServer.ZkTreeListTest.StorageManagerInMemory
import util.Utils
import util.db.{KeyValueDatabaseInMemory, KeyValueDatabaseManagerInMemory}

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

  "ZkMultipleTreeListReaderTest" should "asdsd" in {
    val stream = generateStream

    val producerTransactionsNumber = 100

    val initialTime = 0L
    var atomicLong = new AtomicLong(initialTime)

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

//    atomicLong.set(System.currentTimeMillis())

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

    val (zkServer, zkClient) = Utils.startZkServerAndGetIt
    val zkTreeList1 = new ZookeeperTreeListLong(zkClient, "/treeList1")
    val zkTreeList2 = new ZookeeperTreeListLong(zkClient, "/treeList2")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val testReader = new ZkMultipleTreeListReader(
      Array(zkTreeList1, zkTreeList2),
      storage
    )

    val (records, newData) = testReader.process(Array.empty[LedgerIDAndItsLastRecordID])
    val (records1, newData1) = testReader.process(newData)
    println(newData1.mkString("; "))
    println(records.length, records1.length)

    zkClient.close()
    zkServer.close()
  }

}
