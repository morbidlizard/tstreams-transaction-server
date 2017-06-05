package it

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.server.transactionIDService.TransactionIDService
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, ServerBuilder, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import util.Utils

import scala.concurrent.Await
import scala.concurrent.duration._

class ServerProducerTransactionNotificationTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
{

  private val serverStorageOptions = ServerOptions.StorageOptions(
    path = Files.createTempDirectory("dbs_").toFile.getPath
  )
  private val commitLogToBerkeleyDBTaskDelayMs = 100
  private val serverBuilder = new ServerBuilder()
   .withCommitLogOptions(ServerOptions.CommitLogOptions(
    commitLogCloseDelayMs = commitLogToBerkeleyDBTaskDelayMs
   ))
    .withServerStorageOptions(serverStorageOptions)

  private val clientBuilder = new ClientBuilder()

  private val rand = scala.util.Random
  private def getRandomStream = Utils.getRandomStream
  val secondsWait = 5

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))
  }

  implicit object ProducerTransactionSortable extends Ordering[ProducerTransaction] {
    override def compare(x: ProducerTransaction, y: ProducerTransaction): Int = {
      if (x.stream > y.stream) 1
      else if (x.stream < y.stream) -1
      else if (x.partition > y.partition) 1
      else if (x.partition < y.partition) -1
      else if (x.transactionID > y.transactionID) 1
      else if (x.transactionID < y.transactionID) -1
      else if (x.state.value < y.state.value) -1
      else if (x.state.value > y.state.value) 1
      else 0
    }
  }


  "Client" should "put producer transaction and get notification of it." in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val producerTransactionOuter = ProducerTransaction(streamID, 1, System.currentTimeMillis(), TransactionStates.Opened, 2, 120)

    val latch = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction => producerTransaction.transactionID == producerTransactionOuter.transactionID, latch.countDown())

    bundle.client.putProducerState(producerTransactionOuter)

    latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true
    bundle.close()
  }


  it should "shouldn't get notification." in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)
    val stream = getRandomStream
    Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val latch = new CountDownLatch(1)
    val id = bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction => producerTransaction.transactionID == 10L, latch.countDown())

    latch.await(1, TimeUnit.SECONDS) shouldBe false

    bundle.close()

    bundle.transactionServer.removeNotification(id) shouldBe true
  }

  it should " put producerTransaction with Opened state and don't get it as it's expired" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)
    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val partition = 1

    val ttl = 2
    val producerTransactionOuter = ProducerTransaction(streamID, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

    val latch1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID,
      latch1.countDown()
    )

    val latch2 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID && producerTransaction.state == TransactionStates.Invalid,
      latch2.countDown()
    )

    bundle.client.putProducerState(producerTransactionOuter)
    latch1.await(4, TimeUnit.SECONDS) shouldBe true

    //server checking transactions on expiration periodically
    latch2.await(4, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(bundle.client.getTransaction(streamID, partition, producerTransactionOuter.transactionID), secondsWait.seconds)

    bundle.close()

    res.exists shouldBe true
    res.transaction.get shouldBe ProducerTransaction(streamID, partition, producerTransactionOuter.transactionID, TransactionStates.Invalid, 0, 0L)
  }

  it should "put producerTransaction with Opened state and should get it" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)
    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val partition = 1


    val ttl = TimeUnit.SECONDS.toMillis(5)
    val producerTransactionOuter = ProducerTransaction(streamID, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

    val latch1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID,
      latch1.countDown()
    )

    bundle.client.putProducerState(producerTransactionOuter)
    latch1.await(2, TimeUnit.SECONDS) shouldBe true

    val latch2 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID && producerTransaction.state == TransactionStates.Opened,
      latch2.countDown()
    )

    bundle.client.putProducerState(producerTransactionOuter.copy(state = TransactionStates.Updated))
    latch2.await(2, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(bundle.client.getTransaction(streamID, partition, producerTransactionOuter.transactionID), secondsWait.seconds)

    bundle.close()

    res.exists shouldBe true
    res.transaction.get shouldBe producerTransactionOuter
  }

  //TODO work on test, it failes sometimes
  it should "put 2 producer transactions with opened states and then checkpoint them and should get the second checkpointed transaction" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)
    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val transactionID1 = System.currentTimeMillis()
    bundle.client.putProducerState(ProducerTransaction(streamID, partition, transactionID1, TransactionStates.Opened, 0, 5000L))
    bundle.client.putProducerState(ProducerTransaction(streamID, partition, transactionID1, TransactionStates.Checkpointed, 0, 5000L))

    val transactionID2 = System.currentTimeMillis() + 10L
    bundle.client.putProducerState(ProducerTransaction(streamID, partition, transactionID2, TransactionStates.Opened, 0, 5000L))

    val latch2 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == transactionID2 && producerTransaction.state == TransactionStates.Checkpointed,
      latch2.countDown()
    )

    val producerTransaction2 = ProducerTransaction(streamID, partition, transactionID2, TransactionStates.Checkpointed, 0, 5000L)
    bundle.client.putProducerState(producerTransaction2)

    latch2.await(3, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(bundle.client.getTransaction(streamID, partition, transactionID2), secondsWait.seconds)

    bundle.close()

    res._2.get shouldBe producerTransaction2
    res.exists shouldBe true
  }

  it should "put 'simple' producerTransaction and should get it" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val latch1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.state == TransactionStates.Checkpointed,
      latch1.countDown()
    )

    val transactionID = Await.result(
      bundle.client.putSimpleTransactionAndData(streamID, partition, Seq(Array[Byte]())),
      secondsWait.seconds
    )

    latch1.await(3, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(bundle.client.getTransaction(streamID, partition, transactionID), secondsWait.seconds)

    bundle.close()

    res._2.get shouldBe ProducerTransaction(streamID, partition, transactionID, TransactionStates.Checkpointed, 1, 120L)
    res.exists shouldBe true
  }

  it should "[fire and forget policy] put 'simple' producerTransaction and should get it" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val latch1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.state == TransactionStates.Checkpointed,
      latch1.countDown()
    )

    bundle.client.putSimpleTransactionAndDataWithoutResponse(streamID, partition, Seq(Array.emptyByteArray))

    latch1.await(3, TimeUnit.SECONDS) shouldBe true

    bundle.close()
  }

  it should "put 'simple' producer transactions and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val ALL = 80

    val partition = 1
    val data = Array.fill(10)(rand.nextInt(100000).toString.getBytes)

    val from = TransactionIDService.getTransaction()

    val transactions = (0 until ALL).map { _ =>
      bundle.client.putSimpleTransactionAndData(streamID, partition, data)
    }


    Thread.sleep(2000)
    val to = TransactionIDService.getTransaction()

    val res = Await.result(bundle.client.scanTransactions(
      streamID, partition, from, to, Int.MaxValue, Set(TransactionStates.Opened)
    ), secondsWait.seconds)

    val resData = Await.result(
      bundle.client.getTransactionData(streamID, partition,  res.producerTransactions.last.transactionID, 0, 10),
      secondsWait.seconds
    )

    bundle.close()

    res.producerTransactions.size shouldBe transactions.size
    resData should contain theSameElementsInOrderAs data
  }

  it should "[fire and forget policy] put 'simple' producer transactions and should get them all" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val ALL = 80

    val partition = 1
    val from = TransactionIDService.getTransaction()
    val data = Array.fill(10)(rand.nextInt(100000).toString.getBytes)
    (0 until ALL).foreach { _ =>
      bundle.client.putSimpleTransactionAndDataWithoutResponse(streamID, partition, data)
    }

    Thread.sleep(5000)
    val to = TransactionIDService.getTransaction()

    val res = Await.result(
      bundle.client.scanTransactions(
        streamID, partition, from, to, Int.MaxValue, Set(TransactionStates.Opened)
    ), secondsWait.seconds)

    val resData = Await.result(
      bundle.client.getTransactionData(streamID, partition,  res.producerTransactions.last.transactionID, 0, 10),
      secondsWait.seconds
    )

    bundle.close()

    res.producerTransactions.size shouldBe ALL
    resData should contain theSameElementsInOrderAs data
  }

  it should "return all transactions if no incomplete" in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val putCounter = new CountDownLatch(1)

    val ALL = 80
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last


    bundle.transactionServer.notifyProducerTransactionCompleted(
      t => t.transactionID == lastTransaction && t.state == TransactionStates.Checkpointed,
      putCounter.countDown()
    )

    val partition = 1
    transactions.foreach { t =>
      val openedTransaction = ProducerTransaction(streamID, partition, t, TransactionStates.Opened, 1, 120L)
      bundle.client.putProducerState(openedTransaction)
      bundle.client.putProducerState(openedTransaction.copy(state = TransactionStates.Checkpointed))
    }

    putCounter.await(3000, TimeUnit.MILLISECONDS) shouldBe true

    val res = Await.result(
      bundle.client.scanTransactions(
        streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened)
      ), secondsWait.seconds)

    bundle.close()

    res.producerTransactions.size shouldBe transactions.size
  }



  it should "return checkpointed transaction after client sent different transactions on different partitions." in {
    val bundle = Utils.startTransactionServerAndClient(serverBuilder, clientBuilder)

    val stream = getRandomStream
    val streamID = Await.result(bundle.client.putStream(stream), secondsWait.seconds)

    val firstTransaction0 = System.currentTimeMillis()
    val firstTransaction1 = System.currentTimeMillis() + 10L
    val firstTransaction2 = System.currentTimeMillis() + 124L

    val rootTransaction1 = ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Opened, 1, 120L)
    val rootTransaction2 = ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Opened, 1, 120L)

    bundle.client.putProducerState(rootTransaction1)
    bundle.client.putProducerState(rootTransaction2)

    val putCounter1 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(t =>
      t.partition == 1 && t.transactionID ==
        firstTransaction1 && t.state ==
        TransactionStates.Checkpointed,
      putCounter1.countDown()
    )

    val putCounter2 = new CountDownLatch(1)
    bundle.transactionServer.notifyProducerTransactionCompleted(t =>
      t.partition == 2 && t.transactionID ==
        firstTransaction2 && t.state ==
        TransactionStates.Checkpointed,
      putCounter2.countDown()
    )

    val ALL = 4000
    (0 to ALL) foreach{_=>
      bundle.client.putProducerState(ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Updated, 1, 120L))
      bundle.client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Updated, 1, 120L))
    }

    bundle.client.putProducerState(ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Checkpointed, 1, 120L))
    bundle.client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Checkpointed, 1, 120L))
    putCounter1.await(5000, TimeUnit.MILLISECONDS) shouldBe true
    putCounter2.await(5000, TimeUnit.MILLISECONDS) shouldBe true

    val firstTransaction00 = System.currentTimeMillis()
    val rootTransaction00 = ProducerTransaction(streamID, 0, firstTransaction00, TransactionStates.Opened, 1, 120L)

    val firstTransaction22 = System.currentTimeMillis()
    val rootTransaction22 = ProducerTransaction(streamID, 2, firstTransaction22, TransactionStates.Opened, 1, 120L)

    bundle.client.putProducerState(rootTransaction00)
    bundle.client.putProducerState(rootTransaction22)

    val ALL1 = 4000
    (0 to ALL1) foreach{_=>
      bundle.client.putProducerState(ProducerTransaction(streamID, 0, firstTransaction00, TransactionStates.Updated, 1, 120L))
      bundle.client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction22, TransactionStates.Updated, 1, 120L))
    }

    val res = Await.result(bundle.client.scanTransactions(streamID, 1, firstTransaction1 - 45L , firstTransaction1, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)

    bundle.close()

    res.producerTransactions.head shouldBe ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Checkpointed, 1, 120L)
  }
}
