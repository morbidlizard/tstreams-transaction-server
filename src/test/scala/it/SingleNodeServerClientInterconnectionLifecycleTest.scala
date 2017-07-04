package it

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.StreamDatabaseZK
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.RocksStorageOptions
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import util.Utils.startZkServerAndGetIt

class SingleNodeServerClientInterconnectionLifecycleTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  private val serverBuilder = new SingleNodeServerBuilder()
  private val storageOptions = serverBuilder.getStorageOptions
  private val authOptions = serverBuilder.getAuthenticationOptions
  private val secondsWait = 5

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
  }

  override def afterEach(): Unit = beforeEach()
  private val path = "/tts/test_path"

  it should "put stream, then delete this stream, and put it again and return correct result" in {
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamAfterDelete = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, Some("Previous one was deleted"), 538L)

    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)
    transactionServer.delStream(stream.name)
    val streamAfterDeleteWithID = transactionServer.putStream(streamAfterDelete.name, streamAfterDelete.partitions, streamAfterDelete.description, streamAfterDelete.ttl)

    val retrievedStream = transactionServer.getStream(streamAfterDelete.name).get

    com.bwsw.tstreamstransactionserver.rpc.Stream(
      streamAfterDeleteWithID,
      streamAfterDelete.name,
      streamAfterDelete.partitions,
      streamAfterDelete.description,
      streamAfterDelete.ttl,
      s"$path/ids/id0000000001"
    ) shouldBe retrievedStream

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  private final def getProducerTransactionFromServer(transactionServer: TransactionServer, producerTransaction: ProducerTransaction) = {
    transactionServer.scanTransactions(producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID, producerTransaction.transactionID, Int.MaxValue, Set())
      .producerTransactions.head
  }

  private lazy val fileIDGen = new AtomicLong(0L)
  private final def transitOneTransactionToAnotherState(transactionServiceServer: TransactionServer, in: ProducerTransaction, toUpdateIn: ProducerTransaction, out: ProducerTransaction, timeBetweenTransactionMs: Long) = {
    val firstCommitTime = System.currentTimeMillis()
    val inAggregated = ProducerTransactionRecord(
      in,
      firstCommitTime
    )

    val commitFirst = transactionServiceServer.getBigCommit(fileIDGen.getAndIncrement())
    commitFirst.putProducerTransactions(Seq(inAggregated))
    commitFirst.commit()

    val secondCommitTime = System.currentTimeMillis()
    val toUpdateInAggregated = ProducerTransactionRecord(
      toUpdateIn,
      secondCommitTime + timeBetweenTransactionMs
    )

    val secondCommit = transactionServiceServer.getBigCommit(fileIDGen.getAndIncrement())
    secondCommit.putProducerTransactions(Seq(toUpdateInAggregated))
    secondCommit.commit()

    getProducerTransactionFromServer(transactionServiceServer, out) shouldBe out
  }


  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Checkpointed Transaction" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(2)
    val checkpointedTTL = TimeUnit.SECONDS.toMillis(3)
    val producerTransaction = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL
    )

    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTransaction.stream, producerTransaction.partition,
      producerTransaction.transactionID,
      TransactionStates.Checkpointed, -1,
      checkpointedTTL
    )

    transitOneTransactionToAnotherState(
      transactionServer,
      producerTransaction,
      producerTransactionCheckpointed,
      producerTransactionCheckpointed,
      openedTTL - TimeUnit.SECONDS.toMillis(1)
    )

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(4)
    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2)
    val producerTransaction = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL
    )

    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(
      producerTransaction.stream, producerTransaction.partition, producerTransaction.transactionID,
      TransactionStates.Checkpointed, -1, checkpointedTTL
    )
    
    transitOneTransactionToAnotherState(
      transactionServer,
      producerTransaction,
      producerTransactionCheckpointed,
      producerTransaction.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), openedTTL + TimeUnit.SECONDS.toMillis(1))

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Checkpointed Transaction" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = openedTTL
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)

    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened, openedTTL - TimeUnit.SECONDS.toMillis(2))

    val updatedTTL2 = openedTTL
    val producerTxnUpdated2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated2, producerTxnOpened, updatedTTL2 - TimeUnit.SECONDS.toMillis(2))

    val updatedTTL3 = openedTTL
    val producerTxnUpdated3 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened, updatedTTL3 - TimeUnit.SECONDS.toMillis(2))

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(6)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)

    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTransactionCheckpointed, producerTransactionCheckpointed, checkpointedTTL - TimeUnit.SECONDS.toMillis(2))

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }


  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Updated->Updated->Checkpointed. Should return Invalid Transaction(due to expiration)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID = transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = TimeUnit.SECONDS.toMillis(5L)
    val wait1 = openedTTL - TimeUnit.SECONDS.toMillis(1)
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened.copy(ttl = updatedTTL1), wait1)

    val updatedTTL2 = TimeUnit.SECONDS.toMillis(2L)
    val wait2 = updatedTTL2 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated2, producerTxnOpened.copy(ttl = updatedTTL2), wait2)

    val updatedTTL3 = TimeUnit.SECONDS.toMillis(7L)
    val wait3 = updatedTTL3 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated3 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), wait3)

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2L)
    val wait4 = checkpointedTTL - TimeUnit.SECONDS.toMillis(2)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTransactionCheckpointed, producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), wait4)

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }

  it should "put stream, then put producerTransaction with states in following order: Opened->Updated->Cancel->Updated->Checkpointed. Should return Invalid Transaction(due to transaction with Cancel state)" in {
    val rocksStorageOptions = RocksStorageOptions()
    val serverExecutionContext = new ServerExecutionContextGrids(2, 2)
    val (zkServer, zkClient) = startZkServerAndGetIt
    val streamDatabaseZK = new StreamDatabaseZK(zkClient, path)
    val transactionServer = new TransactionServer(
      executionContext = serverExecutionContext,
      authOpts = authOptions,
      storageOpts = storageOptions,
      rocksStorageOpts = rocksStorageOptions,
      streamDatabaseZK
    )
    val stream = com.bwsw.tstreamstransactionserver.rpc.StreamValue("stream_test", 10, None, 100L)
    val streamID =transactionServer.putStream(stream.name, stream.partitions, stream.description, stream.ttl)


    val openedTTL = TimeUnit.SECONDS.toMillis(7L)
    val updatedTTL1 = TimeUnit.SECONDS.toMillis(4L)
    val wait1 = openedTTL - TimeUnit.SECONDS.toMillis(1)
    val producerTxnOpened = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(streamID, stream.partitions, System.currentTimeMillis(), TransactionStates.Opened, -1, openedTTL)
    val producerTxnUpdated1 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL1)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated1, producerTxnOpened.copy(ttl = updatedTTL1), wait1)

    val updatedTTL2 = TimeUnit.SECONDS.toMillis(1L)
    val wait2 = TimeUnit.SECONDS.toMillis(1L)
    val producerTxnCancel2 = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Cancel, -1, updatedTTL2)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened.copy(ttl = updatedTTL1), producerTxnCancel2, producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), wait2)

    val updatedTTL3 = TimeUnit.SECONDS.toMillis(7L)
    val wait3 = updatedTTL3 - TimeUnit.SECONDS.toMillis(2)
    val producerTxnUpdated3 =com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Updated, -1, updatedTTL3)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTxnUpdated3, producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), wait3)

    val checkpointedTTL = TimeUnit.SECONDS.toMillis(2L)
    val wait4 = checkpointedTTL - TimeUnit.SECONDS.toMillis(2)
    val producerTransactionCheckpointed = com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction(producerTxnOpened.stream, producerTxnOpened.partition, producerTxnOpened.transactionID, TransactionStates.Checkpointed, -1, checkpointedTTL)
    transitOneTransactionToAnotherState(transactionServer, producerTxnOpened, producerTransactionCheckpointed, producerTxnOpened.copy(state = TransactionStates.Invalid, quantity = 0, ttl = 0L), wait4)

    transactionServer.stopAccessNewTasksAndAwaitAllCurrentTasksAreCompletedAndCloseDatabases()
    zkClient.close()
    zkServer.close()
  }
}
