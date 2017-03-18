package it

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ProducerTransaction, Transaction, TransactionStates}

import scala.concurrent.{Await, Future => ScalaFuture}
import scala.concurrent.duration._

class ServerScanTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterEach {

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


  it should "correctly return producerTransactions and it's state(completed or partial) on: LT < A: return (false, Nil), " +
    "where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 5

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)


    streams foreach { stream =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(stream, 1, Long.MaxValue)
      val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] =
        Array(
          (transactionRootChain, currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 1L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Invalid), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 4L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement())
        )

      val transactionsWithTimestamp = producerTransactionsWithTimestamp.map{case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp)}

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 2L , 4L), 5.seconds)

      result.producerTransactions shouldBe empty
      result.isResponseCompleted  shouldBe false
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions and it's state(completed or partial) on: LT < A: return (false, Nil), " +
    "where A - from transaction bound, B - to transaction bound. No transaction had been persisted on server before calling scanTransactions" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 5

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)


    streams foreach { stream =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(stream, 1, Long.MaxValue)
      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 2L , 4L), 5.seconds)

      result.producerTransactions shouldBe empty
      result.isResponseCompleted  shouldBe false
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions and it's state(completed or partial) on: A <= LT < B: return (false, AvailableTransactions[A, LT]), " +
    "where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 5

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)


    streams foreach { stream =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(stream, 1, Long.MaxValue)
      val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] =
        Array(
          (transactionRootChain, currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 1L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 4L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement())
        )

      val transactionsWithTimestamp = producerTransactionsWithTimestamp.map{case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp)}

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 4L), 5.seconds)

      result.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result.isResponseCompleted  shouldBe false
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions and it's state(completed or partial) on: LT >= B: return (true, AvailableTransactions[A, B]), " +
    "where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 5

    val transactionService = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    Await.ready(ScalaFuture.sequence(streams.map(stream =>
      transactionService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    ).toSeq)(implicitly, scala.concurrent.ExecutionContext.Implicits.global), secondsAwait.seconds)


    streams foreach { stream =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(stream, 1, Long.MaxValue)
      val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] =
        Array(
          (transactionRootChain, currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 1L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 2L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 4L, state = TransactionStates.Updated), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 5L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement())
        )

      val transactionsWithTimestamp = producerTransactionsWithTimestamp.map{case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp)}

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result1 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 4L), 5.seconds)
      result1.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result1.isResponseCompleted  shouldBe true

      val result2 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 5L), 5.seconds)
      result2.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1,producerTransactionsWithTimestamp.last._1)
      result2.isResponseCompleted  shouldBe true
    }
    transactionService.shutdown()
  }

}
