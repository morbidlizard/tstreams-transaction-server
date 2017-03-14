package it

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Server, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, RocksStorageOptions}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Transaction, TransactionStates}

import scala.concurrent.duration._
import scala.concurrent.Await

class ServerClientInterconnectionLifecycleTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val serverBuilder = new ServerBuilder()
  private val clientBuilder = new ClientBuilder()
  private val storageOptions = serverBuilder.getStorageOptions()

  override def afterEach() {
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.streamDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + "/" + storageOptions.metadataDirectory))
  }

  private val rand = scala.util.Random
  val secondsWait = 5


//  it should "put stream, then delete this stream, and put it again and return correct result" in {
//    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
//    val streamAfterDelete = transactionService.rpc.Stream("stream_test", 10, Some("Previous one was deleted"), 538L)
//
//    Await.result(client.putStream(stream), secondsWait.seconds)
//    Await.result(client.delStream(stream), secondsWait.seconds)
//    Await.result(client.putStream(streamAfterDelete), secondsWait.seconds)
//
//    val retrievedStream = Await.result(client.getStream(streamAfterDelete.name), secondsWait.seconds)
//
//    streamAfterDelete shouldBe retrievedStream
//  }

  private def getProducerTransactionFromServer(transactionServer: TransactionServer, txn: Transaction) = {
    val producerTransaction = txn._1.get
    Await.result(transactionServer.scanTransactions(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID,  producerTransaction.transactionID), 5.seconds).head
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
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTransactionAggregated = Transaction(Some(producerTransaction), None)

    val commitFirst = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla1")
    commitFirst.putSomeTransactions(Seq((producerTransactionAggregated, System.currentTimeMillis())))
    commitFirst.commit()

    TimeUnit.SECONDS.sleep(openedTTL - 1)
    getProducerTransactionFromServer(transactionServiceServer, producerTransactionAggregated) shouldBe producerTransactionAggregated

    val checkpointedTTL = 3
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    val producerTransactionCheckpointedAggregated = Transaction(Some(producerTransactionCheckpointed) ,None)

    val secondCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla2")
    secondCommit.putSomeTransactions(Seq((producerTransactionCheckpointedAggregated, System.currentTimeMillis())))
    secondCommit.commit()

    TimeUnit.SECONDS.sleep(checkpointedTTL+1)
    getProducerTransactionFromServer(transactionServiceServer, producerTransactionCheckpointedAggregated) shouldBe producerTransactionCheckpointedAggregated
    transactionServiceServer.shutdown()
  }

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
    val producerTransaction = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTransactionAggregated = Transaction(Some(producerTransaction), None)

    val commitFirst = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla1")
    commitFirst.putSomeTransactions(Seq((producerTransactionAggregated, System.currentTimeMillis())))
    commitFirst.commit()

    TimeUnit.SECONDS.sleep(openedTTL + 1)
    getProducerTransactionFromServer(transactionServiceServer, producerTransactionAggregated) shouldBe producerTransactionAggregated

    val checkpointedTTL = 2
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    val producerTransactionCheckpointedAggregated = Transaction(Some(producerTransactionCheckpointed) ,None)

    val secondCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla2")
    secondCommit.putSomeTransactions(Seq((producerTransactionCheckpointedAggregated, System.currentTimeMillis())))
    secondCommit.commit()

    TimeUnit.SECONDS.sleep(checkpointedTTL)
    getProducerTransactionFromServer(transactionServiceServer, producerTransactionCheckpointedAggregated) shouldBe Transaction(Some(transactionService.rpc.ProducerTransaction(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, TransactionStates.Invalid, -1, 0)),None)
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

    val openedTTL = 7
    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTransactionAggregated = Transaction(Some(producerTxnOpened), None)

    val commitFirst = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla1")
    commitFirst.putSomeTransactions(Seq((producerTransactionAggregated, System.currentTimeMillis())))
    commitFirst.commit()

    TimeUnit.SECONDS.sleep(openedTTL - 2)
    getProducerTransactionFromServer(transactionServiceServer, producerTransactionAggregated) shouldBe producerTransactionAggregated

    val updatedTTL1 = openedTTL
    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    val producerTxnUpdated1Aggregated = Transaction(Some(producerTxnUpdated1), None)

    val secondCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla2")
    secondCommit.putSomeTransactions(Seq((producerTxnUpdated1Aggregated, System.currentTimeMillis())))
    secondCommit.commit()

    TimeUnit.SECONDS.sleep(updatedTTL1 - 2)
    getProducerTransactionFromServer(transactionServiceServer, producerTxnUpdated1Aggregated) shouldBe producerTransactionAggregated

    val updatedTTL2 = openedTTL
    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    val producerTxnUpdated2Aggregated = Transaction(Some(producerTxnUpdated2), None)

    val thirdCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla3")
    thirdCommit.putSomeTransactions(Seq((producerTxnUpdated2Aggregated, System.currentTimeMillis())))
    thirdCommit.commit()

    TimeUnit.SECONDS.sleep(updatedTTL2 - 2)
    getProducerTransactionFromServer(transactionServiceServer, producerTxnUpdated2Aggregated) shouldBe producerTransactionAggregated

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    val producerTxnUpdated3Aggregated = Transaction(Some(producerTxnUpdated3), None)

    val forthCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla4")
    forthCommit.putSomeTransactions(Seq((producerTxnUpdated3Aggregated, System.currentTimeMillis())))
    forthCommit.commit()

    TimeUnit.SECONDS.sleep(updatedTTL3 - 2)
    getProducerTransactionFromServer(transactionServiceServer, producerTxnUpdated3Aggregated) shouldBe producerTransactionAggregated

    val checkpointedTTL = 6
    val producerTransactionCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    val producerTransactionCheckpointedAggregated = Transaction(Some(producerTransactionCheckpointed), None)

    val fifthCommit = transactionServiceServer.getBigCommit(System.currentTimeMillis(), "/tmp/blabla5")
    fifthCommit.putSomeTransactions(Seq((producerTransactionCheckpointedAggregated, System.currentTimeMillis())))
    fifthCommit.commit()

    TimeUnit.SECONDS.sleep(checkpointedTTL - 1)

    getProducerTransactionFromServer(transactionServiceServer, producerTransactionCheckpointedAggregated) shouldBe producerTransactionCheckpointedAggregated
    transactionServiceServer.shutdown()
  }
//
//  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
//    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
//    Await.result(client.putStream(stream), secondsWait.seconds)
//
//    val openedTTL = 6
//    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
//    Await.result(client.putTransaction(producerTxnOpened), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnOpened) shouldBe producerTxnOpened
//
//    val updatedTTL1 = 4
//    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
//    Await.result(client.putTransaction(producerTxnUpdated1), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnUpdated1) shouldBe transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Opened, -1, updatedTTL1)
//
//
//    val producerTxnInvalid = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Invalid, -1, 0)
//    val updatedTTL2 = 2
//    val producerTxnUpdated2 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
//    Await.result(client.putTransaction(producerTxnUpdated2), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnUpdated2) shouldBe producerTxnInvalid
//
//    val updatedTTL3 = openedTTL
//    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
//    Await.result(client.putTransaction(producerTxnUpdated3), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnUpdated3) shouldBe producerTxnInvalid
//
//    val checkpointedTTL = 2
//    val producerTxnCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
//    Await.result(client.putTransaction(producerTxnCheckpointed), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnCheckpointed) shouldBe producerTxnInvalid
//  }
//
//  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Cancel->Updated->Checkpointed. Should return Invalid Transaction(due to transaction with Cancel state)" in {
//    val stream = transactionService.rpc.Stream("stream_test", 10, None, 100L)
//    Await.result(client.putStream(stream), secondsWait.seconds)
//
//    val openedTTL = 6
//    val producerTxnOpened = transactionService.rpc.ProducerTransaction(stream.name,stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
//    Await.result(client.putTransaction(producerTxnOpened), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnOpened) shouldBe producerTxnOpened
//
//    val updatedTTL1 = 4
//    val producerTxnUpdated1 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
//    Await.result(client.putTransaction(producerTxnUpdated1), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnUpdated1) shouldBe transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Opened, -1, updatedTTL1)
//
//
//    val producerTxnInvalid = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Invalid, -1, 0)
//    val updatedTTL2 = 2
//    val producerTxnCancel = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Cancel, -4, updatedTTL2)
//    Await.result(client.putTransaction(producerTxnCancel), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnCancel) shouldBe producerTxnInvalid
//
//    val updatedTTL3 = openedTTL
//    val producerTxnUpdated3 = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
//    Await.result(client.putTransaction(producerTxnUpdated3), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnUpdated3) shouldBe producerTxnInvalid
//
//    val checkpointedTTL = 2
//    val producerTxnCheckpointed = transactionService.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
//    Await.result(client.putTransaction(producerTxnCheckpointed), secondsWait.seconds)
//
//    Thread.sleep(5000)
//    getProducerTransactionFromServer(producerTxnCheckpointed) shouldBe producerTxnInvalid
//  }

}
