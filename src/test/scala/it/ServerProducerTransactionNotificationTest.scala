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
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(commitLogCloseDelayMs = commitLogToBerkeleyDBTaskDelayMs)
  private val serverPackageTransmissionOptions = ServerOptions.TransportOptions()
  private val serverZookeeperSpecificOptions = ServerOptions.ZooKeeperOptions()

  def startTransactionServer(): Unit = new Thread(() => {
    val serverZookeeperOptions = CommonOptions.ZookeeperOptions(endpoints = zkTestServer.getConnectString)
    transactionServer = new Server(
      authOpts = serverAuthOptions,
      zookeeperOpts = serverZookeeperOptions,
      serverOpts = serverBootstrapOptions,
      serverReplicationOpts = serverReplicationOptions,
      storageOpts = serverStorageOptions,
      berkeleyStorageOptions = serverBerkeleyStorageOptions,
      rocksStorageOpts = serverRocksStorageOptions,
      commitLogOptions = serverCommitLogOptions,
      zookeeperSpecificOpts = serverZookeeperSpecificOptions,
      packageTransmissionOpts = serverPackageTransmissionOptions
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))

    zkTestServer = new TestingServer(true)
    startTransactionServer()
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
    val commitLogCatalogue = new CommitLogCatalogue(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory)
    commitLogCatalogue.deleteAllFiles()
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
    new com.bwsw.tstreamstransactionserver.rpc.Stream {
      override val name: String = rand.nextInt(10000).toString
      override val partitions: Int = rand.nextInt(10000)
      override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
      override val ttl: Long = Long.MaxValue
    }

  val secondsWait = 5

  "Client" should "put producer transaction and get notification of it." in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactionOuter = ProducerTransaction(stream.name, 1, System.currentTimeMillis(), TransactionStates.Opened, 2, 120)

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

  it should "producerTransaction with Opened state and don't get it as it's expired" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val partition = 1

    val ttl = 2
    val producerTransactionOuter = ProducerTransaction(stream.name, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

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

    val res = Await.result(client.getTransaction(stream.name, partition, producerTransactionOuter.transactionID), secondsWait.seconds)
    res.exists shouldBe true
    res.transaction.get shouldBe ProducerTransaction(stream.name, partition, producerTransactionOuter.transactionID, TransactionStates.Invalid, 0, 0L)
  }

  it should "producerTransaction with Opened state and should get it" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val partition = 1

    val ttl = 5
    val producerTransactionOuter = ProducerTransaction(stream.name, partition, System.currentTimeMillis(), TransactionStates.Opened, 2, ttl)

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

    val res = Await.result(client.getTransaction(stream.name, partition, producerTransactionOuter.transactionID), secondsWait.seconds)
    res.exists shouldBe true
    res.transaction.get shouldBe producerTransactionOuter
  }

  it should "return all transactions if no incomplete" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

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
      val openedTransaction = ProducerTransaction(stream.name, partition, t, TransactionStates.Opened, 1, 120L)
      client.putProducerState(openedTransaction)
      client.putProducerState(openedTransaction.copy(state = TransactionStates.Checkpointed))
    }

    putCounter.await(3000, TimeUnit.MILLISECONDS) shouldBe true

    val res = Await.result(client.scanTransactions(stream.name, partition, firstTransaction, lastTransaction), secondsWait.seconds)

    res.producerTransactions.size shouldBe transactions.size
  }

}
