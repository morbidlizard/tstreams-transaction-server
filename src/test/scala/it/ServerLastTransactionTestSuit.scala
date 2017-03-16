package it

import java.io.File

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ProducerTransaction, Transaction, TransactionStates}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{Future => ScalaFuture}
import scala.language.reflectiveCalls

class ServerLastTransactionTestSuit extends FlatSpec with Matchers with BeforeAndAfterEach {

  private val rand = scala.util.Random

  private def getRandomStream = transactionService.rpc.Stream(
    name = rand.nextInt(10000).toString,
    partitions = rand.nextInt(10000),
    description = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None,
    ttl = Long.MaxValue
  )

  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream, txnID: Long, ttlTxn: Long) = ProducerTransaction(
    stream = streamObj.name,
    partition = streamObj.partitions,
    transactionID = txnID,
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

  it should "correctly return last transaction id per stream and partition" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 50
    val producerTxnPerStreamPartitionMaxNumber = 100

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    ) {
      final def getLastTransactionIDWrapper(stream: String, partition: Int) = {
        val streamObj = getStreamFromOldestToNewest(stream).last
        getLastTransactionID(streamObj.streamNameToLong, partition)
      }
    }

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)


    streams foreach { stream =>
      val producerTransactionsNumber = rand.nextInt(producerTxnPerStreamPartitionMaxNumber)

      val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] = (0 to producerTransactionsNumber).map { transactionID =>
        val producerTransaction = getRandomProducerTransaction(stream, transactionID.toLong, Long.MaxValue)
        (producerTransaction, System.currentTimeMillis())
      }.toArray

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val transactionsWithTimestamp = producerTransactionsWithTimestamp.map { case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp) }

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      transactionService.getLastTransactionIDWrapper(stream.name, stream.partitions).get shouldBe maxTransactionID

    }
    transactionService.shutdown()
  }

  it should "correctly return last transaction id per stream and partition even if some transactions are out of order." in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 2
    val producerTxnPerStreamPartitionMaxNumber = 50

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    ) {
      final def getLastTransactionIDWrapper(stream: String, partition: Int) = {
        val streamObj = getStreamFromOldestToNewest(stream).last
        getLastTransactionID(streamObj.streamNameToLong, partition)
      }
    }

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)

    @tailrec
    def getLastTransactionID(producerTransactions: List[ProducerTransaction], acc: Option[Long]): Option[Long] = {
      producerTransactions match {
        case Nil => acc
        case txn :: txns =>
          if (acc.isEmpty) getLastTransactionID(txns, acc)
          else if (acc.get <= txn.transactionID) getLastTransactionID(txns, Some(txn.transactionID))
          else getLastTransactionID(txns, acc)
      }
    }

    streams foreach { stream =>
      val producerTransactionsNumber = rand.nextInt(producerTxnPerStreamPartitionMaxNumber)


      val producerTransactions = scala.util.Random.shuffle(0 to producerTransactionsNumber).map { transactionID =>
        val transaction = getRandomProducerTransaction(stream, transactionID.toLong, Long.MaxValue)
        (transaction, System.currentTimeMillis() + rand.nextInt(100))
      }.toList

      val producerTransactionsOrderedByTimestamp = producerTransactions.sortBy(_._2)
      val transactionsWithTimestamp = producerTransactionsOrderedByTimestamp.map { case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp) }

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      transactionService.getLastTransactionIDWrapper(stream.name, stream.partitions) shouldBe getLastTransactionID(producerTransactionsOrderedByTimestamp.map(_._1), Some(0L))
    }
    transactionService.shutdown()
  }
}
