package it

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

import com.bwsw.commitlog.filesystem.CommitLogCatalogueByFolder
import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.{Server, Time}
import com.bwsw.tstreamstransactionserver.options.{ClientBuilder, CommonOptions, ServerOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions._
import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.rpc._

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

  private object TestTimer extends Time {
    private val initialTime = System.currentTimeMillis()
    private var currentTime = initialTime

    override def getCurrentTime: Long = currentTime
    def resetTimer(): Unit = currentTime = initialTime
    def updateTime(newTime: Long) = currentTime = newTime
  }

  private val maxIdleTimeBetweenRecordsMs = 1000

  private val serverAuthOptions = ServerOptions.AuthOptions()
  private val serverBootstrapOptions = ServerOptions.BootstrapOptions()
  private val serverReplicationOptions = ServerOptions.ServerReplicationOptions()
  private val serverStorageOptions = ServerOptions.StorageOptions()
  private val serverBerkeleyStorageOptions = ServerOptions.BerkeleyStorageOptions()
  private val serverRocksStorageOptions = ServerOptions.RocksStorageOptions()
  private val serverCommitLogOptions = ServerOptions.CommitLogOptions(maxIdleTimeBetweenRecordsMs = maxIdleTimeBetweenRecordsMs, commitLogCloseDelayMs = Int.MaxValue)
  private val serverPackageTransmissionOptions = ServerOptions.TransportOptions()

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
      packageTransmissionOpts = serverPackageTransmissionOptions,
      timer = TestTimer
    )
    transactionServer.start()
  }).start()


  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(serverStorageOptions.path + java.io.File.separatorChar + serverStorageOptions.commitLogDirectory))

    TestTimer.resetTimer()
    zkTestServer = new TestingServer(true)
    startTransactionServer()
    Thread.sleep(30)
    client = clientBuilder.withZookeeperOptions(ZookeeperOptions(endpoints = zkTestServer.getConnectString)).build()
  }

  override def afterEach() {
    TestTimer.resetTimer()
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

  it should "put stream, then delete that stream and check it doesn't exist" in {
    val stream = getRandomStream

    Await.result(client.putStream(stream), secondsWait.seconds) shouldBe true
    Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe true
    Await.result(client.delStream(stream), secondsWait.seconds) shouldBe true
    Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe false
  }

  it should "put stream, then delete that stream, then again delete this stream and get that operation isn't successful" in {
    val stream = getRandomStream

    Await.result(client.putStream(stream), secondsWait.seconds) shouldBe true
    Await.result(client.delStream(stream), secondsWait.seconds) shouldBe true
    Await.result(client.checkStreamExists(stream.name), secondsWait.seconds) shouldBe false
    Await.result(client.delStream(stream), secondsWait.seconds)  shouldBe false
  }

  it should "put stream, then delete this stream, and server shouldn't save producer and consumer transactions on putting them by client" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened)
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

    //it's required to close a current commit log file
    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)

    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)

    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
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

    val producerTransactions = Array.fill(100000)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    transactionServer.shutdown()

    val timeToWait = clientBuilder.getConnectionOptions.connectionTimeoutMs.milliseconds
    assertThrows[java.util.concurrent.TimeoutException] {
      Await.result(resultInFuture, timeToWait)
    }
  }

  it should "not throw an exception when the server isn't available for time less than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(100)(getRandomProducerTransaction(stream))
    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)

    transactionServer.shutdown()
    TimeUnit.MILLISECONDS.sleep(clientBuilder.getConnectionOptions.connectionTimeoutMs * 3 / 5)
    startTransactionServer()

    Await.result(resultInFuture, secondsWait.seconds) shouldBe true
  }

  it should "put any kind of binary data and get it back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val txn = getRandomProducerTransaction(stream)
    Await.result(client.putProducerState(txn), secondsWait.seconds)

    val dataAmount = 5000
    val data = Array.fill(dataAmount)(rand.nextString(10).getBytes)

    val resultInFuture = Await.result(client.putTransactionData(txn.stream, txn.partition, txn.transactionID, data, 0), secondsWait.seconds)
    resultInFuture shouldBe true

    val dataFromDatabase = Await.result(client.getTransactionData(txn.stream, txn.partition, txn.transactionID, 0, dataAmount), secondsWait.seconds)
    data should contain theSameElementsAs dataFromDatabase
  }

  it should "put transactions and get them back(scanTransactions)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val producerTransactions = Array.fill(30)(getRandomProducerTransaction(stream)).filter(_.state == TransactionStates.Opened) :+
      getRandomProducerTransaction(stream).copy(state = TransactionStates.Opened)

    val consumerTransactions = Array.fill(100)(getRandomConsumerTransaction(stream))

    Await.result(client.putTransactions(producerTransactions, consumerTransactions), secondsWait.seconds)

    val statesAllowed = Array(TransactionStates.Opened, TransactionStates.Updated)
    val (from, to) = (
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).minBy(_.transactionID).transactionID,
      producerTransactions.filter(txn => statesAllowed.contains(txn.state)).maxBy(_.transactionID).transactionID
    )

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)

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

    val producerOpenedTransactions = producerTransactionsByState(TransactionStates.Opened).sortBy(_.transactionID)

    res.head shouldBe producerOpenedTransactions.head
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
    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)

    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)

    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
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
    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
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
    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    //it's required to a CommitLogToBerkeleyWriter writes the producer transactions to db
    transactionServer.berkeleyWriter.run()

    val response = Await.result(client.getTransaction(stream.name, stream.partitions, openedProducerTransaction.transactionID), secondsWait.seconds)

    //assert
    response shouldBe TransactionInfo(exists = true, Some(openedProducerTransaction))
  }

  it should "put consumerCheckpoint and get a transaction id back" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val consumerTransaction = getRandomConsumerTransaction(stream)

    Await.result(client.putConsumerCheckpoint(consumerTransaction), secondsWait.seconds)
    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()

    val consumerState = Await.result(client.getConsumerState(consumerTransaction.name, consumerTransaction.stream, consumerTransaction.partition), secondsWait.seconds)

    consumerState shouldBe consumerTransaction.transactionID
  }

  "Server" should "not have any problems with many clients" in {
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

  "Server" should "return only transactions up to 1st incomplete(transaction after Opened one)" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream), secondsWait.seconds)

    val FIRST = 30
    val LAST = 100
    val partition = 1

    val transactions1 = for (i <- 0 until FIRST) yield {
      TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
      TestTimer.getCurrentTime
    }

    Await.result(client.putTransactions(transactions1.flatMap { t =>
      Array(
        ProducerTransaction(stream.name, partition, t, TransactionStates.Opened, 1, 120L),
        ProducerTransaction(stream.name, partition, t, TransactionStates.Checkpointed, 1, 120L)
      )
    }, Seq()), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()

    TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
    Await.result(client.putProducerState(ProducerTransaction(stream.name, partition,  TestTimer.getCurrentTime, TransactionStates.Opened, 1, 120L)), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()

    val transactions2 = for (i <- FIRST until LAST) yield {
      TestTimer.updateTime(TestTimer.getCurrentTime + 1L)
      TestTimer.getCurrentTime
    }

    Await.result(client.putTransactions(transactions2.flatMap { t =>
      Array(
        ProducerTransaction(stream.name, partition, t, TransactionStates.Opened, 1, 120L),
        ProducerTransaction(stream.name, partition, t, TransactionStates.Checkpointed, 1, 120L)
      )
    }, Seq()), secondsWait.seconds)

    TestTimer.updateTime(TestTimer.getCurrentTime + maxIdleTimeBetweenRecordsMs)
    Await.result(client.putConsumerCheckpoint(getRandomConsumerTransaction(stream)), secondsWait.seconds)
    transactionServer.berkeleyWriter.run()

    val transactions = transactions1 ++ transactions2
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last

    val res = Await.result(client.scanTransactions(stream.name, partition, firstTransaction, lastTransaction), secondsWait.seconds)

    res.producerTransactions.size shouldBe transactions1.size + 1
  }
}
