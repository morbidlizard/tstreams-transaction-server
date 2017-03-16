package it


import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.Key
import com.bwsw.tstreamstransactionserver.options.ServerOptions._
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ProducerTransaction, Transaction, TransactionStates}
import scala.language.reflectiveCalls
import scala.concurrent.duration._
import scala.concurrent.Await

class ServerCleanerTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private val rand = scala.util.Random

  private def getRandomStream = transactionService.rpc.Stream(
    name = rand.nextInt(10000).toString,
    partitions = rand.nextInt(10000),
    description = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None,
    ttl = Long.MaxValue
  )

  private val txnCounter = new AtomicLong(0)

  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream, ttlTxn: Long) = ProducerTransaction(
    stream = streamObj.name,
    partition = streamObj.partitions,
    transactionID = txnCounter.getAndIncrement(),
    state = TransactionStates.Opened,
    quantity = -1,
    ttl = ttlTxn
  )

  private val storageOptions = StorageOptions(path = "/tmp")

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.streamDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
  }

  override def afterEach() {
    beforeEach()
  }


  "Cleaner" should "remove all expired transactions from OpenedTransactions table and invalidate them in AllTransactions table" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5
    val maxTTLForProducerTransactionSec = 5

    val producerTxnNumber = 100

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    ) {
      def checkTransactionExistInOpenedTable(stream: String, partition: Int, transactionId: Long) = {
        val streamObj = getStreamFromOldestToNewest(stream).last
        val txn = getOpenedTransaction(Key(streamObj.streamNameToLong, partition, transactionId))
        txn.isDefined
      }
    }

    def ttlSec = rand.nextInt(maxTTLForProducerTransactionSec)

    val stream = getRandomStream
    Await.ready(transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsAwait.seconds)

    val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] = Array.fill(producerTxnNumber) {
      val producerTransaction = getRandomProducerTransaction(stream, ttlSec)
      (producerTransaction, System.currentTimeMillis())
    }
    val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
    val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

    val transactionsWithTimestamp = producerTransactionsWithTimestamp.map { case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp) }

    val bigCommit = transactionService.getBigCommit(System.currentTimeMillis(), storageOptions.path)
    bigCommit.putSomeTransactions(transactionsWithTimestamp)
    bigCommit.commit()

    TimeUnit.SECONDS.sleep(maxTTLForProducerTransactionSec)
    transactionService.createTransactionsToDeleteTask(System.currentTimeMillis()).run()

    val expiredTransactions = producerTransactionsWithTimestamp.map { case (producerTxn, _) =>
      val invalidTransaction = ProducerTransaction(producerTxn.stream, producerTxn.partition, producerTxn.transactionID, TransactionStates.Invalid, producerTxn.quantity, 0L)
      Transaction(Some(invalidTransaction), None)
    }

    Await.result(transactionService.scanTransactions(stream.name, stream.partitions, minTransactionID, maxTransactionID), secondsAwait.seconds) should contain theSameElementsAs expiredTransactions

    (minTransactionID to maxTransactionID) foreach { transactionID =>
      transactionService.checkTransactionExistInOpenedTable(stream.name, stream.partitions, transactionID) shouldBe false
    }

    transactionService.shutdown()
  }

}
