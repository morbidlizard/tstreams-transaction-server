package it

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import Utils._


class ServerScanTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterEach {

  private val rand = scala.util.Random

  private def getRandomStream = com.bwsw.tstreamstransactionserver.rpc.Stream(
    name = rand.nextInt(10000).toString,
    partitions = rand.nextInt(10000),
    description = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None,
    ttl = Long.MaxValue
  )

  private def getRandomProducerTransaction(streamID:Int, streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream, txnID: Long, ttlTxn: Long) = ProducerTransaction(
    stream = streamID,
    partition = streamObj.partitions,
    transactionID = txnID,
    state = TransactionStates.Opened,
    quantity = -1,
    ttl = ttlTxn
  )

  private val storageOptions = StorageOptions(path = "/tmp")

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogDirectory))
  }

  override def afterEach() {
    beforeEach()
  }

  private val path = "/tts/streams"

  it should "correctly return producerTransactions on: LT < A: " +
    "return (LT, Nil), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamID, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamID, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result = transactionServer.scanTransactions(streamID, stream.partitions, 2L , 4L, Int.MaxValue, Set(TransactionStates.Opened))

      result.producerTransactions shouldBe empty
      result.lastOpenedTransactionID shouldBe 3L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkServer.close()
    zkClient.close()
  }

  it should "correctly return producerTransactions on: LT < A: " +
    "return (LT, Nil), where A - from transaction bound, B - to transaction bound. " +
    "No transactions had been persisted on server before scanTransactions was called" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )

    streamsAndIDs foreach {case (streamID, stream) =>
      val result = transactionServer.scanTransactions(streamID, stream.partitions, 2L , 4L, Int.MaxValue, Set(TransactionStates.Opened))

      result.producerTransactions shouldBe empty
      result.lastOpenedTransactionID shouldBe -1L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkServer.close()
    zkClient.close()
  }

  it should "correctly return producerTransactions on: A <= LT < B: " +
    "return (LT, AvailableTransactions[A, LT]), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamID, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamID, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val result = transactionServer.scanTransactions(streamID, stream.partitions, 0L , 4L, Int.MaxValue, Set(TransactionStates.Opened))

      result.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result.lastOpenedTransactionID shouldBe 3L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "correctly return producerTransactions until first opened and not checkpointed transaction on: A <= LT < B: " +
    "return (LT, AvailableTransactions[A, LT]), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamId, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamId, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val result = transactionServer.scanTransactions(streamId, stream.partitions, 0L , 5L, Int.MaxValue, Set(TransactionStates.Opened))

      result.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1)
      result.lastOpenedTransactionID shouldBe 5L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "correctly return producerTransactions on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamId, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamId, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val result1 = transactionServer.scanTransactions(streamId, stream.partitions, 0L , 4L, Int.MaxValue, Set(TransactionStates.Opened))
      result1.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result1.lastOpenedTransactionID  shouldBe 5L

      val result2 = transactionServer.scanTransactions(streamId, stream.partitions, 0L , 5L, Int.MaxValue, Set(TransactionStates.Opened))
      result2.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result2.lastOpenedTransactionID shouldBe 5L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "correctly return producerTransactions with defined count and states(which discard all producers transactions thereby retuning an empty collection of them) on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamId, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamId, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)
      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result2 = transactionServer.scanTransactions(streamId, stream.partitions, 0L , 5L, 0, Set(TransactionStates.Opened))
      result2.producerTransactions shouldBe empty
      result2.lastOpenedTransactionID  shouldBe 5L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "correctly return producerTransactions with defined count and states(which accepts all producers transactions thereby retuning all of them) on: LT >= B: " +
    "return (LT, AvailableTransactions[A, B]), where A - from transaction bound, B - to transaction bound" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamId, stream) =>
      val currentTimeInc = new AtomicLong(System.currentTimeMillis())
      val transactionRootChain = getRandomProducerTransaction(streamId, stream, 1, Long.MaxValue)
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
      val bigCommit = transactionServer.getBigCommit(1L)

      bigCommit.putSomeTransactions(transactionsWithTimestamp)
      bigCommit.commit()

      val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
      val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

      val result2 = transactionServer.scanTransactions(streamId, stream.partitions, 0L , 5L, 5, Set(TransactionStates.Opened))
      result2.producerTransactions should contain theSameElementsAs Seq(producerTransactionsWithTimestamp(1)._1, producerTransactionsWithTimestamp(6)._1)
      result2.lastOpenedTransactionID  shouldBe 5L
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "return all transactions if no incomplete" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val stream = getRandomStream
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val ALL = 80
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last


    val partition = 1
    val txns = transactions.flatMap { t =>
      Seq(
        (Transaction(Some(ProducerTransaction(streamID, partition, t, TransactionStates.Opened, 1, 120L)), None), t),
        (Transaction(Some(ProducerTransaction(streamID, partition, t, TransactionStates.Checkpointed, 1, 120L)), None), t)
      )
    }

    val bigCommit1 = transactionServer.getBigCommit(1L)
    bigCommit1.putSomeTransactions(txns)
    bigCommit1.commit()

    val res = transactionServer.scanTransactions(streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened))

    res.producerTransactions.size shouldBe transactions.size

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }


  it should "return only transactions up to 1st incomplete(transaction after Opened one)" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val streamsNumber = 1

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val streams = Array.fill(streamsNumber)(getRandomStream)
    val streamsAndIDs = streams.map(stream =>
      (transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), stream)
    )


    streamsAndIDs foreach {case (streamId, stream) =>
      val FIRST = 30
      val LAST = 100
      val partition = 1

      var currentTime = System.currentTimeMillis()
      val transactions1 = for (i <- 0 until FIRST) yield {
        currentTime = currentTime + 1L
        currentTime
      }


      val bigCommit1 = transactionServer.getBigCommit(1L)
      bigCommit1.putSomeTransactions(transactions1.flatMap { t =>
        Seq(
          (Transaction(Some(ProducerTransaction(streamId, partition, t, TransactionStates.Opened, 1, 120L)), None), t ),
          (Transaction(Some(ProducerTransaction(streamId, partition, t, TransactionStates.Checkpointed, 1, 120L)), None), t)
        )
      })
      bigCommit1.commit()


      val bigCommit2 = transactionServer.getBigCommit(2L)
      bigCommit2.putSomeTransactions(Seq((Transaction(Some(ProducerTransaction(streamId, partition, currentTime, TransactionStates.Opened, 1, 120L)), None), currentTime)))
      bigCommit2.commit()

      val transactions2 = for (i <- FIRST until LAST) yield {
        currentTime = currentTime + 1L
        currentTime
      }

      val bigCommit3 = transactionServer.getBigCommit(3L)
      bigCommit3.putSomeTransactions(transactions1.flatMap { t =>
        Seq(
          (Transaction(Some(ProducerTransaction(streamId, partition, t, TransactionStates.Opened, 1, 120L)), None), t),
          (Transaction(Some(ProducerTransaction(streamId, partition, t, TransactionStates.Checkpointed, 1, 120L)), None), t)
        )
      })
      bigCommit3.commit()

      val transactions = transactions1 ++ transactions2
      val firstTransaction = transactions.head
      val lastTransaction = transactions.last

      val res = transactionServer.scanTransactions(streamId, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened))
      res.producerTransactions.size shouldBe transactions1.size
    }
    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "return none if empty" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val stream = getRandomStream
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val ALL = 100
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }

    val firstTransaction = transactions.head
    val lastTransaction = transactions.last
    val res = transactionServer.scanTransactions(streamID, 1, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened))
    res.producerTransactions.size shouldBe 0

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "return none if to < from" in {
    val authOptions = com.bwsw.tstreamstransactionserver.options.ServerOptions.AuthOptions()
    val storageOptions = StorageOptions()
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 2)

    val secondsAwait = 5

    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )

    val stream = getRandomStream
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val ALL = 80

    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.tail.tail.tail.head

    val bigCommit1 = transactionServer.getBigCommit(1L)
    bigCommit1.putSomeTransactions(transactions.flatMap { t =>
        Seq((Transaction(Some(ProducerTransaction(streamID, 1, t, TransactionStates.Opened, 1, 120L)), None), t)
      )
    })
    bigCommit1.commit()


    val res = transactionServer.scanTransactions(streamID, 1, lastTransaction, firstTransaction, Int.MaxValue, Set(TransactionStates.Opened))
    res.producerTransactions.size shouldBe 0

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }
}
