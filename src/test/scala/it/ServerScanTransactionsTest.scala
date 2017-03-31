package it

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future => ScalaFuture}

class ServerScanTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private val rand = scala.util.Random

  private def getRandomStream = com.bwsw.tstreamstransactionserver.rpc.Stream(
    name = rand.nextInt(10000).toString,
    partitions = rand.nextInt(10000),
    description = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None,
    ttl = Long.MaxValue
  )

  private def getRandomProducerTransaction(streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream, txnID: Long, ttlTxn: Long) = ProducerTransaction(
    stream = streamObj.name,
    partition = streamObj.partitions,
    transactionID = txnID,
    state = TransactionStates.Opened,
    quantity = -1,
    ttl = ttlTxn
  )

  private val storageOptions = StorageOptions(path = "/tmp")

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
  }

  override def afterEach() {
    beforeEach()
  }


  it should "correctly return producerTransactions on: LT < A: " +
    "return (LT, Nil), where A - from transaction bound, B - to transaction bound" in {
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
      result.lastOpenedTransactionID shouldBe 3L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions on: LT < A: " +
    "return (LT, Nil), where A - from transaction bound, B - to transaction bound. " +
    "No transactions had been persisted on server before scanTransactions was called" in {
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
      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 2L , 4L), 5.seconds)

      result.producerTransactions shouldBe empty
      result.lastOpenedTransactionID shouldBe -1L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions on: A <= LT < B: " +
    "return (LT, AvailableTransactions[A, LT]), where A - from transaction bound, B - to transaction bound" in {
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

      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 4L), 5.seconds)

      result.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result.lastOpenedTransactionID shouldBe 3L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions until first opened and not checkpointed transaction on: A <= LT < B: " +
    "return (LT, AvailableTransactions[A, LT]), where A - from transaction bound, B - to transaction bound" in {
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
          (transactionRootChain.copy(transactionID = 3L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 5L, state = TransactionStates.Opened), currentTimeInc.getAndIncrement()),
          (transactionRootChain.copy(transactionID = 5L, state = TransactionStates.Checkpointed), currentTimeInc.getAndIncrement())
        )

      val transactionsWithTimestamp = producerTransactionsWithTimestamp.map{case (producerTxn, timestamp) => (Transaction(Some(producerTxn), None), timestamp)}

      val currentTime = System.currentTimeMillis()
      val bigCommit = transactionService.getBigCommit(storageOptions.path)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit(currentTime)

      val result = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 5L), 5.seconds)

      result.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(2)._1)
      result.lastOpenedTransactionID shouldBe 5L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
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

      val result1 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 4L), 5.seconds)
      result1.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result1.lastOpenedTransactionID  shouldBe 5L

      val result2 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 5L), 5.seconds)
      result2.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1,producerTransactionsWithTimestamp.last._1)
      result2.lastOpenedTransactionID shouldBe 5L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions with defined lambda(which discard all producers transactions thereby retuning an empty collection of them) on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
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

      val result2 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 5L, txn => txn.transactionID > 5L), 5.seconds)
      result2.producerTransactions shouldBe empty
      result2.lastOpenedTransactionID  shouldBe 5L
    }
    transactionService.shutdown()
  }

  it should "correctly return producerTransactions with defined lambda(which accepts all producers transactions thereby retuning all of them) on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
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

      val result2 = Await.result(transactionService.scanTransactions(stream.name, stream.partitions, 0L , 5L, txn => txn.transactionID <= 5L), 5.seconds)
      result2.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1,producerTransactionsWithTimestamp.last._1)
      result2.lastOpenedTransactionID  shouldBe 5L
    }
    transactionService.shutdown()
  }


  it should "return only transactions up to 1st incomplete(transaction after Opened one)" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)

    val secondsAwait = 5

    val streamsNumber = 1

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
      val FIRST = 30
      val LAST = 100
      val partition = 1

      var currentTime = System.currentTimeMillis()

      val transactions1 = for (i <- 0 until FIRST) yield {
        currentTime = currentTime + 1L
        currentTime
      }


      val bigCommit1 = transactionService.getBigCommit(storageOptions.path)
      bigCommit1.putSomeTransactions(transactions1.flatMap { t =>
        Seq(
          (Transaction(Some(ProducerTransaction(stream.name, partition, t, TransactionStates.Opened, 1, 120L)), None), t ),
          (Transaction(Some(ProducerTransaction(stream.name, partition, t, TransactionStates.Checkpointed, 1, 120L)), None), t)
        )
      })
      bigCommit1.commit(currentTime)


      val bigCommit2 = transactionService.getBigCommit(storageOptions.path)
      bigCommit2.putSomeTransactions(Seq((Transaction(Some(ProducerTransaction(stream.name, partition, currentTime, TransactionStates.Opened, 1, 120L)), None), currentTime)))
      bigCommit2.commit(currentTime)

      val transactions2 = for (i <- FIRST until LAST) yield {
        currentTime = currentTime + 1L
        currentTime
      }

      val bigCommit3 = transactionService.getBigCommit(storageOptions.path)
      bigCommit3.putSomeTransactions(transactions1.flatMap { t =>
        Seq(
          (Transaction(Some(ProducerTransaction(stream.name, partition, t, TransactionStates.Opened, 1, 120L)), None), t),
          (Transaction(Some(ProducerTransaction(stream.name, partition, t, TransactionStates.Checkpointed, 1, 120L)), None), t)
        )
      })
      bigCommit3.commit(currentTime)

      val transactions = transactions1 ++ transactions2
      val firstTransaction = transactions.head
      val lastTransaction = transactions.last

      val res = Await.result(transactionService.scanTransactions(stream.name, partition, firstTransaction, lastTransaction), 5.seconds)
      res.producerTransactions.size shouldBe transactions1.size
    }
    transactionService.shutdown()
  }

}
