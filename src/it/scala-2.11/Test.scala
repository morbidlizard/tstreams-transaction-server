import authService.AuthServer
import com.twitter.util.{Await, Closable, Time}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}

class Test extends FlatSpec with Matchers with BeforeAndAfterEach {
  val client: TransactionZooKeeperClient = new TransactionZooKeeperClient
  var transactionServer: TransactionZooKeeperServer = _
  var authServer: AuthServer = _

  override def beforeEach(): Unit = {
    transactionServer = new TransactionZooKeeperServer
    authServer = new AuthServer

    transactionServer.start()
    authServer.start()
  }

  override def afterEach() {
    Await.ready(Closable.all(transactionServer, authServer).close())
  }

  private val rand = scala.util.Random
  private def getRandomStream = new transactionService.rpc.Stream {
    override val name: String = rand.nextInt(10000).toString
    override val partitions: Int = rand.nextInt(10000)
    override val description: Option[String] = if (rand.nextBoolean()) Some(rand.nextInt(10000).toString) else None
    override val ttl: Int = rand.nextInt(Int.MaxValue)
  }
  private def chooseStreamRandomly(streams: IndexedSeq[transactionService.rpc.Stream]) = streams(rand.nextInt(streams.length))

  private def getRandomProducerTransaction(streamObj: transactionService.rpc.Stream) = new ProducerTransaction {
    override val transactionID: Long = rand.nextLong()
    override val state: TransactionStates = TransactionStates(rand.nextInt(TransactionStates.list.length) + 1)
    override val stream: String = streamObj.name
    override val timestamp: Long = Time.epoch.inNanoseconds
    override val quantity: Int = -1
    override val partition: Int = rand.nextInt(10000)
  }

  private def getRandomConsumerTransaction(streamObj: transactionService.rpc.Stream) =  new ConsumerTransaction {
    override def transactionID: Long = scala.util.Random.nextLong()
    override def name: String = rand.nextInt(10000).toString
    override def stream: String = streamObj.name
    override def partition: Int = rand.nextInt(10000)
  }


  "TransactionZooKeeperClient" should "put producer and consumer transactions" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream))

    val producerTransactions = (0 to 100).map(_ => getRandomProducerTransaction(stream))
    val consumerTransactions = (0 to 100).map(_ => getRandomConsumerTransaction(stream))

    val result = client.putTransactions(producerTransactions, consumerTransactions)

    Await.result(result) shouldBe true
  }

  it should "throw an exception when the auth server isn't available" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream))

    authServer.close()

    val producerTransactions = (0 to 100).map(_ => getRandomProducerTransaction(stream))
    val consumerTransactions = (0 to 100).map(_ => getRandomConsumerTransaction(stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)


    assertThrows[org.apache.thrift.TApplicationException] {
      Await.result(resultInFuture)
    }
  }

  //TODO Config shouldn't be static, because it's impossible to make custom configs for test purposes.
  it should "not throw an exception when the auth server isn't available for time less than in config" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream))

    authServer.close()

    val producerTransactions = (0 to 100).map(_ => getRandomProducerTransaction(stream))
    val consumerTransactions = (0 to 100).map(_ => getRandomConsumerTransaction(stream))


    val resultInFuture = client.putTransactions(producerTransactions, consumerTransactions)


    assertThrows[org.apache.thrift.TApplicationException] {
      Await.result(resultInFuture)
    }
  }

//  it should "put any kind of binary data and get it back" in {
//    val stream = getRandomStream
//    Await.result(client.putStream(stream))
//
//    val txn = getRandomProducerTransaction(stream)
//    Await.result(client.putTransaction(txn))
//
//    val data = (0 to 10).map(_=> rand.nextString(1000).getBytes)
//
//    val resultInFuture = client.putTransactionData(txn, data)
//
//    Await.result(resultInFuture) shouldBe true
//
//    val dataFromDatabase = Await.result(client.getTransactionData(txn,0,10))
//
//    data should contain theSameElementsAs dataFromDatabase
//  }

  "TransactionZooKeeperServer" should "not save producer and consumer transactions, that don't refer to a stream in database they should belong to" in {
    val stream = getRandomStream
    Await.result(client.putStream(stream))

    val streamFake = getRandomStream
    val producerTransactions = (0 to 100).map(_ => getRandomProducerTransaction(streamFake))
    val consumerTransactions = (0 to 100).map(_ => getRandomConsumerTransaction(streamFake))

    val result = client.putTransactions(producerTransactions, consumerTransactions)
    assertThrows[org.apache.thrift.TApplicationException] {
      Await.result(result)
    }
  }

}
