package it

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.options.ServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, RocksStorageOptions}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ProducerTransaction, Transaction, TransactionStates}

import scala.concurrent.Await
import scala.concurrent.duration._

class ServerClientInterconnectionLifecycleTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val serverBuilder = new ServerBuilder()
  private val storageOptions = serverBuilder.getStorageOptions()
  private val secondsWait = 5

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.streamDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
  }

  override def afterEach(): Unit = beforeEach()

  it should "put stream, then delete this stream, and put it again and return correct result" in {
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    val streamAfterDelete = transactionService.rpc.Stream("stream_test", 10, Some("Previous one was deleted"), 538L)

    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)
    Await.result(transactionServiceServer.delStream(stream.name), secondsWait.seconds)
    Await.result(transactionServiceServer.putStream(streamAfterDelete.name, streamAfterDelete.partitions, streamAfterDelete.description, streamAfterDelete.ttl), secondsWait.seconds)

    val retrievedStream = Await.result(transactionServiceServer.getStream(streamAfterDelete.name), secondsWait.seconds)

    streamAfterDelete shouldBe retrievedStream
    transactionServiceServer.shutdown()
  }

  private final def getProducerTransactionFromServer(transactionServer: TransactionServer, txn: Transaction) = {
    val producerTransaction = txn._1.get
    Await.result(
      transactionServer.scanTransactions(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, producerTransaction.transactionID),
      5.seconds
    ).head._1.get
  }

  private final def transitOneTransactionToAnotherState(transactionServiceServer: TransactionServer, in: ProducerTransaction, toUpdateIn: ProducerTransaction, out: ProducerTransaction, timeBeetwenTransactionSec: Long) = {
    val inAggregated = Transaction(Some(in), None)
    val firstCommitTime = System.currentTimeMillis()
    val commitFirst = transactionServiceServer.getBigCommit(scala.util.Random.nextString(6))
    commitFirst.putSomeTransactions(Seq((inAggregated, firstCommitTime)))
    commitFirst.commit(firstCommitTime)

    val toUpdateInAggregated = Transaction(Some(toUpdateIn), None)
    val secondCommitTime = System.currentTimeMillis()
    val secondCommit = transactionServiceServer.getBigCommit(scala.util.Random.nextString(6))
    secondCommit.putSomeTransactions(Seq((toUpdateInAggregated, secondCommitTime + TimeUnit.SECONDS.toMillis(timeBeetwenTransactionSec))))
    secondCommit.commit(secondCommitTime)

    getProducerTransactionFromServer(transactionServiceServer, Transaction(Some(out), None)) shouldBe out
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Checkpointed Transaction" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)

    val openedTTL = 2
    val checkpointedTTL = 3
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTransaction, producerTransactionCheckpointed, producerTransactionCheckpointed, openedTTL - 1)

    transactionServiceServer.shutdown()
  }

//  it should "put stream, then put producerTransaction with state: Checkpointed. " +
//    "Should throw an exception (due an invalid transition of state machine)" in {
//    val rocksStorageOptions = RocksStorageOptions()
//    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
//    val transactionServiceServer = new TransactionServer(
//      executionContext = serverExecutionContext,
//      authOpts = AuthOptions(),
//      storageOpts = storageOptions,
//      rocksStorageOpts = rocksStorageOptions
//    )
//    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
//    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)
//
//    val openedTTL = 2
//    val producerTransactionWithInvalidState = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Checkpointed, -1, openedTTL)
//    val out = producerTransactionWithInvalidState.copy(state = TransactionStates.Invalid, ttl = 0L)
//    val inAggregated = Transaction(Some(producerTransactionWithInvalidState), None)
//    val firstCommitTime = System.currentTimeMillis()
//    val commitFirst = transactionServiceServer.getBigCommit(scala.util.Random.nextString(6))
//
//    assertThrows[IllegalArgumentException] {
//      commitFirst.putSomeTransactions(Seq((inAggregated, firstCommitTime)))
//    }
//
//    transactionServiceServer.shutdown()
//  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)

    val openedTTL = 4
    val checkpointedTTL = 2
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTransaction, producerTransactionCheckpointed, producerTransaction.copy(state = TransactionStates.Invalid, ttl = 0L), openedTTL + 1)

    transactionServiceServer.shutdown()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Checkpointed Transaction" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)

    val openedTTL = 7L
    val updatedTTL1 = openedTTL
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened, openedTTL - 2)

    val updatedTTL2 = openedTTL
    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated2, producerTxnOpened, updatedTTL2 - 2)

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened, updatedTTL3 - 2)

    val checkpointedTTL = 6
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTransactionCheckpointed, producerTransactionCheckpointed, checkpointedTTL - 2)

    transactionServiceServer.shutdown()
  }


  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)

    val openedTTL = 7L
    val updatedTTL1 = 5L
    val wait1 = openedTTL - 1
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened.copy(ttl = updatedTTL1), wait1)

    val updatedTTL2 = 2L
    val wait2 = updatedTTL2 - 2
    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated2, producerTxnOpened.copy(ttl = updatedTTL2), wait2)

    val updatedTTL3 = 7L
    val wait3 = updatedTTL3 - 2
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened.copy(state = TransactionStates.Invalid, ttl = 0L), wait3)

    val checkpointedTTL = 2L
    val wait4 = checkpointedTTL - 2
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTransactionCheckpointed, producerTxnOpened.copy(state = TransactionStates.Invalid, ttl = 0L), wait4)

    transactionServiceServer.shutdown()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Cancel->Updated->Checkpointed. Should return Invalid Transaction(due to transaction with Cancel state)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContext(2, 1, 1, 1)
    val transactionServiceServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = AuthOptions(),
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions
    )
    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
    Await.result(transactionServiceServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl), secondsWait.seconds)

    val openedTTL = 7L
    val updatedTTL1 = 4L
    val wait1 = openedTTL - 1
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened.copy(ttl = updatedTTL1), wait1)

    val updatedTTL2 = 1L
    val wait2 = 1L
    val producerTxnCancel2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Cancel, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened.copy(ttl = updatedTTL1), producerTxnCancel2, producerTxnOpened.copy(state = TransactionStates.Invalid, ttl = 0L), wait2)

    val updatedTTL3 = 7L
    val wait3 = updatedTTL3 - 2
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened.copy(state = TransactionStates.Invalid, ttl = 0L), wait3)

    val checkpointedTTL = 2L
    val wait4 = checkpointedTTL - 2
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServiceServer, producerTxnOpened, producerTransactionCheckpointed, producerTxnOpened.copy(state = TransactionStates.Invalid, ttl = 0L), wait4)

    transactionServiceServer.shutdown()
  }
}
