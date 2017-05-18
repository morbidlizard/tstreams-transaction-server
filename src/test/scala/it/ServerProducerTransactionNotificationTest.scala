package it

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class ServerProducerTransactionNotificationTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: Server = _

  private val clientBuilder = new ClientBuilder()

  private val commitLogToBerkeleyDBTaskDelayMs = 100

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(commitLogCloseDelayMs = commitLogToBerkeleyDBTaskDelayMs)
  private val serverPackageTransmissionOptions = ServerOptions.TransportOptions()
  private val serverZookeeperSpecificOptions = ServerOptions.ZooKeeperOptions()

  def startTransactionServer(): Server = {
    val serverZookeeperOptions = CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    transactionServer = new Server(
      authOpts = serverAuthOptions,
      zookeeperOpts = serverZookeeperOptions,
      serverOpts = serverBootstrapOptions,
      serverReplicationOpts = serverReplicationOptions,
      storageOpts = serverStorageOptions,
      rocksStorageOpts = serverRocksStorageOptions,
      commitLogOptions = serverCommitLogOptions,
      zookeeperSpecificOpts = serverZookeeperSpecificOptions,
      packageTransmissionOpts = serverPackageTransmissionOptions
    )
    val l = new CountDownLatch(1)
    new Thread(() => {
      l.countDown()
      transactionServer.start()
    }).start()
    l.await()
    transactionServer
  }


  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))

    zkTestServer = new TestingServer(true)
    startTransactionServer()
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(
      endpoints = zkTestServer.getConnectString)
    ).build()
  }

  override def afterEach() {
    client.shutdown()
    transactionServer.shutdown()
    zkTestServer.close()

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
      else 0
    }
  }

  private val rand = scala.util.Random
  private def getRandomStream =
    new com.bwsw.tstreamstransactionserver.rpc.StreamValue {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
    }

  val secondsWait = 5

  "Client" should "put producer transaction and get notification of it." in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactionOuter = ProducerTransaction(streamID, 1, System.currentTimeMillis(), TransactionStates.Opened, 2, 120)

    val latch = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction => producerTransaction.transactionID == producerTransactionOuter.transactionID, latch.countDown())

    client.putProducerState(producerTransactionOuter)

    latch.await(secondsWait, TimeUnit.SECONDS) shouldBe true
  }


  it should "shouldn't get notification." in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val latch = new CountDownLatch(1)
    val id = transactionServer.notifyProducerTransactionCompleted(producerTransaction => producerTransaction.transactionID == 10L, latch.countDown())

    latch.await(1, TimeUnit.SECONDS) shouldBe false
    transactionServer.removeNotification(id) shouldBe true
  }

  it should " put producerTransaction with Opened state and don't get it as it's expired" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val partition = 1

    val ttl = 2
    val producerTransactionOuter = ProducerTransaction(streamID, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

    val latch1 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID,
      latch1.countDown()
    )

    val latch2 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID && producerTransaction.state == TransactionStates.Invalid,
      latch2.countDown()
    )

    client.putProducerState(producerTransactionOuter)
    latch1.await(4, TimeUnit.SECONDS) shouldBe true

    //server checking transactions on expiration periodically
    latch2.await(4, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(client.getTransaction(streamID, partition, producerTransactionOuter.transactionID), secondsWait.seconds)
    res.exists shouldBe true
    res.transaction.get shouldBe ProducerTransaction(streamID, partition, producerTransactionOuter.transactionID, TransactionStates.Invalid, 0, 0L)
  }

  it should "put producerTransaction with Opened state and should get it" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val partition = 1


    val ttl = TimeUnit.SECONDS.toMillis(5)
    val producerTransactionOuter = ProducerTransaction(streamID, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

    val latch1 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID,
      latch1.countDown()
    )

    client.putProducerState(producerTransactionOuter)
    latch1.await(2, TimeUnit.SECONDS) shouldBe true

    val latch2 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == producerTransactionOuter.transactionID && producerTransaction.state == TransactionStates.Opened,
      latch2.countDown()
    )

    client.putProducerState(producerTransactionOuter.copy(state = TransactionStates.Updated))
    latch2.await(2, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(client.getTransaction(streamID, partition, producerTransactionOuter.transactionID), secondsWait.seconds)
    res.exists shouldBe true
    res.transaction.get shouldBe producerTransactionOuter
  }

  it should "put 2 producer transactions with opened states and then checkpoint them and should get the second checkpointed transaction" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val transactionID1 = System.currentTimeMillis()
    client.putProducerState(ProducerTransaction(streamID, partition, transactionID1, TransactionStates.Opened, 0, 5L))
    client.putProducerState(ProducerTransaction(streamID, partition, transactionID1, TransactionStates.Checkpointed, 0, 5L))

    val transactionID2 = System.currentTimeMillis() + 10L
    client.putProducerState(ProducerTransaction(streamID, partition, transactionID2, TransactionStates.Opened, 0, 5L))

    val latch2 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == transactionID2 && producerTransaction.state == TransactionStates.Checkpointed,
      latch2.countDown()
    )

    val producerTransaction2 = ProducerTransaction(streamID, partition, transactionID2, TransactionStates.Checkpointed, 0, 5L)
    client.putProducerState(producerTransaction2)

    latch2.await(3, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(client.getTransaction(streamID, partition, transactionID2), secondsWait.seconds)
    res._2.get shouldBe producerTransaction2
    res.exists shouldBe true
  }

  it should "put 'simple' producerTransaction and should get it" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val transactionID = System.currentTimeMillis()
    val latch1 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == transactionID && producerTransaction.state == TransactionStates.Checkpointed,
      latch1.countDown()
    )

    client.putSimpleTransactionAndData(streamID, partition, transactionID, Seq(Array[Byte]()))
    latch1.await(3, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(client.getTransaction(streamID, partition, transactionID), secondsWait.seconds)
    res._2.get shouldBe ProducerTransaction(streamID, partition, transactionID, TransactionStates.Checkpointed, 1, 120L)
    res.exists shouldBe true
  }

  it should "[fire and forget policy] put 'simple' producerTransaction and should get it" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)
    val partition = 1

    val transactionID = System.currentTimeMillis()
    val latch1 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(producerTransaction =>
      producerTransaction.transactionID == transactionID && producerTransaction.state == TransactionStates.Checkpointed,
      latch1.countDown()
    )

    client.putSimpleTransactionAndDataWithoutResponse(streamID, partition, transactionID, Seq(Array[Byte]()))
    latch1.await(3, TimeUnit.SECONDS) shouldBe true

    val res = Await.result(client.getTransaction(streamID, partition, transactionID), secondsWait.seconds)
    res._2.get shouldBe ProducerTransaction(streamID, partition, transactionID, TransactionStates.Checkpointed, 1, 120L)
    res.exists shouldBe true
  }

  it should "put 'simple' producer transactions and should get them all" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val putCounter = new CountDownLatch(1)

    val ALL = 80
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last


    transactionServer.notifyProducerTransactionCompleted(t => t.transactionID == lastTransaction && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    val partition = 1
    val data = Array.fill(10)(rand.nextInt(100000).toString.getBytes)
    transactions.foreach { t =>
      client.putSimpleTransactionAndData(streamID, partition, t, data)
    }

    putCounter.await(3000, TimeUnit.MILLISECONDS) shouldBe true

    Thread.sleep(3000)
    val res = Await.result(client.scanTransactions(streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)
    val resData = Await.result(client.getTransactionData(streamID, partition, lastTransaction, 0, 10), secondsWait.seconds)


    res.producerTransactions.size shouldBe transactions.size
    resData should contain theSameElementsInOrderAs data
  }

  it should "[fire and forget policy] put 'simple' producer transactions and should get them all" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val putCounter = new CountDownLatch(1)

    val ALL = 80
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last


    transactionServer.notifyProducerTransactionCompleted(t => t.transactionID == lastTransaction && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    val partition = 1
    val data = Array.fill(10)(rand.nextInt(100000).toString.getBytes)
    transactions.foreach { t =>
      client.putSimpleTransactionAndDataWithoutResponse(streamID, partition, t, data)
    }

    putCounter.await(3000, TimeUnit.MILLISECONDS) shouldBe true

    Thread.sleep(3000)
    val res = Await.result(client.scanTransactions(streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)
    val resData = Await.result(client.getTransactionData(streamID, partition, lastTransaction, 0, 10), secondsWait.seconds)


    res.producerTransactions.size shouldBe transactions.size
    resData should contain theSameElementsInOrderAs data
  }

  it should "return all transactions if no incomplete" in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val putCounter = new CountDownLatch(1)

    val ALL = 80
    var currentTime = System.currentTimeMillis()
    val transactions = for (i <- 0 until ALL) yield {
      currentTime = currentTime + 1L
      currentTime
    }
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last


    transactionServer.notifyProducerTransactionCompleted(t => t.transactionID == lastTransaction && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    val partition = 1
    transactions.foreach { t =>
      val openedTransaction = ProducerTransaction(streamID, partition, t, TransactionStates.Opened, 1, 120L)
      client.putProducerState(openedTransaction)
      client.putProducerState(openedTransaction.copy(state = TransactionStates.Checkpointed))
    }

    putCounter.await(3000, TimeUnit.MILLISECONDS) shouldBe true

    val res = Await.result(client.scanTransactions(streamID, partition, firstTransaction, lastTransaction, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)

    res.producerTransactions.size shouldBe transactions.size
  }



  it should "return checkpointed transaction after client sent different transactions on different partitions." in {
    val stream = getRandomStream
    val streamID = Await.result(client.putStream(stream), secondsWait.seconds)

    val firstTransaction0 = System.currentTimeMillis()
    val firstTransaction1 = System.currentTimeMillis() + 10L
    val firstTransaction2 = System.currentTimeMillis() + 124L

    val rootTransaction1 = ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Opened, 1, 120L)
    val rootTransaction2 = ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Opened, 1, 120L)

    client.putProducerState(rootTransaction1)
    client.putProducerState(rootTransaction2)

    val putCounter1 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(t =>
      t.partition == 1 && t.transactionID ==
        firstTransaction1 && t.state ==
        TransactionStates.Checkpointed,
      putCounter1.countDown()
    )

    val putCounter2 = new CountDownLatch(1)
    transactionServer.notifyProducerTransactionCompleted(t =>
      t.partition == 2 && t.transactionID ==
        firstTransaction2 && t.state ==
        TransactionStates.Checkpointed,
      putCounter2.countDown()
    )

    val ALL = 4000
    (0 to ALL) foreach{_=>
      client.putProducerState(ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Updated, 1, 120L))
      client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Updated, 1, 120L))
    }

    client.putProducerState(ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Checkpointed, 1, 120L))
    client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction2, TransactionStates.Checkpointed, 1, 120L))
    putCounter1.await(5000, TimeUnit.MILLISECONDS) shouldBe true
    putCounter2.await(5000, TimeUnit.MILLISECONDS) shouldBe true

    val firstTransaction00 = System.currentTimeMillis()
    val rootTransaction00 = ProducerTransaction(streamID, 0, firstTransaction00, TransactionStates.Opened, 1, 120L)

    val firstTransaction22 = System.currentTimeMillis()
    val rootTransaction22 = ProducerTransaction(streamID, 2, firstTransaction22, TransactionStates.Opened, 1, 120L)

    client.putProducerState(rootTransaction00)
    client.putProducerState(rootTransaction22)

    val ALL1 = 4000
    (0 to ALL1) foreach{_=>
      client.putProducerState(ProducerTransaction(streamID, 0, firstTransaction00, TransactionStates.Updated, 1, 120L))
      client.putProducerState(ProducerTransaction(streamID, 2, firstTransaction22, TransactionStates.Updated, 1, 120L))
    }

    val res = Await.result(client.scanTransactions(streamID, 1, firstTransaction1 - 45L , firstTransaction1, Int.MaxValue, Set(TransactionStates.Opened)), secondsWait.seconds)
    res.producerTransactions.head == ProducerTransaction(streamID, 1, firstTransaction1, TransactionStates.Checkpointed, 1, 120L)
  }
}
