package it

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

import com.bwsw.commitlog.filesystem.CommitLogCatalogue
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.{HasTime, Server}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions._
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionInfo, TransactionStates}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ServerClientInterconnection extends FlatSpec with Matchers with BeforeAndAfterEach {
  var zkTestServer: TestingServer = _
  var client: Client = _
  var transactionServer: Server = _

  val clientsNum = 2
  private val clientBuilder = new ClientBuilder()

  private object TestTimer extends HasTime {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime
    def resetTimer(): Unit = currentTime = initialTime
    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBeetwenRecords = 1
  private val commitLogToBerkeleyDBTaskDelay = 100

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(maxIdleTimeBetweenRecords = maxIdleTimeBeetwenRecords, commitLogToBerkeleyDBTaskDelayMs = Int.MaxValue)
  private val serverPackageTransmissionOptions = ServerOptions.PackageTransmissionOptions()

  def startTransactionServer() = new Thread(() => {
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
      packageTransmissionOpts = serverPackageTransmissionOptions,
      timer = TestTimer
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    TestTimer.resetTimer()
    zkTestServer = new TestingServer(true)
    startTransactionServer()
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
    val commitLogCatalogue = new CommitLogCatalogue(serverStorageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => catalogue.deleteAllFiles())
  }

  override def afterEach() {
    TestTimer.resetTimer()
    client.shutdown()
    transactionServer.shutdown()
    zkTestServer.close()
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + "/" + serverStorageOptions.metadataDirectory))
    val commitLogCatalogue = new CommitLogCatalogue(serverStorageOptions.path)
    commitLogCatalogue.catalogues.foreach(catalogue => catalogue.deleteAllFiles())
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

  private def chooseStreamRandomly(streams: IndexedSeq[com.bwsw.tstreamstransactionserver.rpc.Stream]) = streams(rand.nextInt(streams.length))

  private def getRandomProducerTransaction(streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream,
                                           transactionState: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1),
                                           id: Long = System.nanoTime()) =
    new ProducerTransaction {
      override val transactionID: Long = id
      override val state: TransactionStates = transactionState
      override val stream: String = streamObj.name
      override val ttl: Long = Long.MaxValue
      override val quantity: Int = -1
      override val partition: Int = streamObj.partitions
    }

  private def getRandomConsumerTransaction(streamObj: com.bwsw.tstreamstransactionserver.rpc.Stream) =
    new ConsumerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()
      override val name: String = rand.nextInt(10000).toString
      override val stream: String = streamObj.name
      override val partition: Int = streamObj.partitions
    }


  val secondsWait = 5

  "Client" should "put producer and consumer transactions" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val result = client.putTransactions(producerTransactions, consumerTransactions)

    Await.result(result, 5.seconds) shouldBe true
  }


  it should "delete stream, that doesn't exist in database on the server and get result" in {
    Await.result(client.delStream(getRandomStream), secondsWait.seconds) shouldBe false
  }

  it should "put stream, then delete this stream, and server shouldn't save producer and consumer transactions on putting them by client" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))

    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)

    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val fromID = producerTransactions.minBy(_.transactionID).transactionID
    val toID = producerTransactions.maxBy(_.transactionID).transactionID

    val resultBeforeDeleting = Await.result(client.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions
    resultBeforeDeleting should not be empty

    Await.result(client.delStream(stream), secondsWait.seconds)
    assertThrows[com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist] {
      Await.result(client.scanTransactions(stream.name, stream.partitions, fromID, toID), secondsWait.seconds).producerTransactions
    }
  }

  it should "throw an exception when the a server isn't available for time greater than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    transactionServer.shutdown()
    assertThrows[java.util.concurrent.TimeoutException] {
      Await.result(resultInFuture, clientBuilder.getConnectionOptions().connectionTimeoutMs.milliseconds)
    }
  }

  it should "not throw an exception when the server isn't available for time less than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    transactionServer.shutdown()
    TimeUnit.MILLISECONDS.sleep(clientBuilder.getConnectionOptions().connectionTimeoutMs * 3 / 5)
    startTransactionServer()

    Await.result(resultInFuture, secondsWait.seconds) shouldBe true
  }

  it should "put any kind of binary data and get it back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val txn = getRandomProducerTransaction(stream)
    Await.result(client.putProducerState(txn), secondsWait.seconds)

    val amount = 5000
    val data = Array.fill(amount)(rand.nextString(10).getBytes)

    val resultInFuture = Await.result(client.putTransactionData(txn.stream, txn.partition, txn.transactionID, data, 0), secondsWait.seconds)
    resultInFuture shouldBe true

    val dataFromDatabase = Await.result(client.getTransactionData(txn.stream, txn.partition, txn.transactionID, 0, amount), secondsWait.seconds)
    data should contain theSameElementsAs dataFromDatabase
  }

  it should "put transactions and get them back(scanTransactions)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(30)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

    val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
    val (from, to) = (
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
    )

    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))

    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()


    val resFrom_1From = Await.result(client.scanTransactions(stream.name, stream.partitions, from - 1, from), secondsWait.seconds)
    resFrom_1From.producerTransactions.size shouldBe 1
    resFrom_1From.producerTransactions.head.transactionID shouldBe from


    val resFromFrom = Await.result(client.scanTransactions(stream.name, stream.partitions, from, from), secondsWait.seconds)
    resFromFrom.producerTransactions.size shouldBe 1
    resFromFrom.producerTransactions.head.transactionID shouldBe from


    val resToFrom = Await.result(client.scanTransactions(stream.name, stream.partitions, to, from), secondsWait.seconds)
    resToFrom.producerTransactions.size shouldBe 0

    val producerTransactionsByState = producerTransactions.groupBy(_.state)
    val res = Await.result(client.scanTransactions(stream.name, stream.partitions, from, to), secondsWait.seconds).producerTransactions

    val txns = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

    res should contain theSameElementsAs txns
    res shouldBe sorted
  }

  "getTransaction" should "not get a producer transaction if there's no transaction" in {
    //arrange
    val stream = getRandomStream
    val fakeTransactionID = System.nanoTime()

    //act
    Await.result(client.putStream(stream), secondsWait.seconds)
    val response = Await.result(client.getTransaction(stream.name, stream.partitions, fakeTransactionID), secondsWait.seconds)

    //assert
    response shouldBe TransactionInfo(exists = false, None)
  }

  it should "put a producer transaction (Opened), return it and shouldn't return a producer transaction which id is greater (getTransaction)" in {
    //arrange
    val stream = getRandomStream
    val openedProducerTransaction = getRandomProducerTransaction(stream, TransactionStates.Opened)
    val fakeTransactionID = openedProducerTransaction.transactionID + 1

    //act
    Await.result(client.putStream(stream), secondsWait.seconds)
    Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))

    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)

    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val successResponse = Await.result(client.getTransaction(stream.name, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)
    val failedResponse = Await.result(client.getTransaction(stream.name, stream.partitions, fakeTransactionID), secondsWait.seconds)

    //assert
    successResponse shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
    failedResponse shouldBe TransactionInfo(exists = false, None)
  }

  it should "put a producer transaction (Opened), return it and shouldn't return a non-existent producer transaction which id is less (getTransaction)" in {
    //arrange
    val stream = getRandomStream
    val openedProducerTransaction = getRandomProducerTransaction(stream, TransactionStates.Opened)
    val fakeTransactionID = openedProducerTransaction.transactionID - 1

    //act
    Await.result(client.putStream(stream), secondsWait.seconds)
    Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val successResponse = Await.result(client.getTransaction(stream.name, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)
    val failedResponse = Await.result(client.getTransaction(stream.name, stream.partitions, fakeTransactionID), secondsWait.seconds)

    //assert
    successResponse shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
    failedResponse shouldBe TransactionInfo(exists = true, None)
  }

  it should "put a producer transaction (Opened) and get it back (getTransaction)" in {
    //arrange
    val stream = getRandomStream
    val openedProducerTransaction = getRandomProducerTransaction(stream, TransactionStates.Opened)

    //act
    Await.result(client.putStream(stream), secondsWait.seconds)
    Await.result(client.putProducerState(openedProducerTransaction), secondsWait.seconds)

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val response = Await.result(client.getTransaction(stream.name, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)

    //assert
    response shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
  }

  it should "put consumerCheckpoint and get transaction id back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val consumerTransaction = getRandomConsumerTransaction(stream)

    Await.result(client.putConsumerCheckpoint(consumerTransaction), secondsWait.seconds)
    TestTimer.updateTime(TestTimer.getCurrentTime + TimeUnit.SECONDS.toMillis(maxIdleTimeBeetwenRecords))
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()

    val consumerState = Await.result(client.getConsumerState(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition), secondsWait.seconds)

    consumerState shouldBe consumerTransaction.transactionID
  }

  "Server" should "not have problems with many clients" in {
    val clients = Array.fill(clientsNum)(clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build())
    val streams = Array.fill(10000)(getRandomStream)
    Await.result(client.putStream(chooseStreamRandomly(streams)), secondsWait.seconds)

    val dataCounter = new java.util.concurrent.ConcurrentHashMap[(String, Int), LongAdder]()

    def addDataLength(stream: String, partition: Int, dataLength: Int): Unit = {
      val valueToAdd = if (dataCounter.containsKey((stream, partition))) dataLength else 0
      dataCounter.computeIfAbsent((stream, partition), (t: (String, Int)) => new LongAdder()).add(valueToAdd)
    }

    def getDataLength(stream: String, partition: Int) = dataCounter.get((stream, partition)).intValue()


    val res: Future[mutable.ArraySeq[Boolean]] = Future.sequence(clients map { client =>
      val streamFake = getRandomStream
      client.putStream(streamFake).flatMap { _ =>
        val producerTransactions = Array.fill(100)(getRandomProducerTransaction(streamFake))
        val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(streamFake))
        val data = Array.fill(100)(rand.nextInt(10000).toString.getBytes)

        client.putTransactions(producerTransactions, consumerTransactions)

        val (stream, partition) = (producerTransactions.head.stream, producerTransactions.head.partition)
        addDataLength(stream, partition, data.length)
        val txn = producerTransactions.head
        client.putTransactionData(txn.stream, txn.partition, txn.transactionID, data, getDataLength(stream, partition))
      }
    })

    all(Await.result(res, (secondsWait * clientsNum).seconds)) shouldBe true
  }
}
