import authService.AuthServer
import com.twitter.util.{Await, Closable, Time, Future => TwitterFuture}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class Test extends FlatSpec with Matchers with BeforeAndAfterEach {
  var transactionServer = new TransactionZooKeeperServer
  var authServer = new AuthServer
  val client = new TransactionZooKeeperClient

  override def beforeEach(): Unit = {
    transactionServer = new TransactionZooKeeperServer
    authServer = new AuthServer
    transactionServer.start()
    authServer.start()
  }

  override def afterEach() {
    Closable.all(transactionServer,authServer).close()
  }

//  "Claeawe" should "qwe" in {
//    import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
//    println(Await.result(client.putStream("1",20, None, 5)))
//
//    val rand = scala.util.Random
//
//    val producerTransactions = (0 to 100000).map(_ => new ProducerTransaction {
//      override val transactionID: Long = rand.nextLong()
//
//      override val state: TransactionStates = TransactionStates.Opened
//
//      override val stream: String = "1"
//
//      override val timestamp: Long = Time.epoch.inNanoseconds
//
//      override val quantity: Int = -1
//
//      override val partition: Int = rand.nextInt(10000)
//    })
//
//    val consumerTransactions = (0 to 100000).map(_ => new ConsumerTransaction {
//      override def transactionID: Long = scala.util.Random.nextLong()
//
//      override def name: String = rand.nextInt(10000).toString
//
//      override def stream: String = "1"
//
//      override def partition: Int = rand.nextInt(10000)
//    })
//
//
//    val resultInFuture = client.putTransactions(producerTransactions, Seq())
//    println(Await.ready(resultInFuture))
//
//   // val data = (0 to 1000000) map (_ => rand.nextString(10).getBytes().toSeq)
//  }
//
//  it should "asdasdasd" in {
//    import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
//    println(Await.result(client.putStream("1",20, None, 5)))
//
//    val rand = scala.util.Random
//
//    val producerTransactions = (0 to 100000).map(_ => new ProducerTransaction {
//      override val transactionID: Long = rand.nextLong()
//
//      override val state: TransactionStates = TransactionStates.Opened
//
//      override val stream: String = "1"
//
//      override val timestamp: Long = Time.epoch.inNanoseconds
//
//      override val quantity: Int = -1
//
//      override val partition: Int = rand.nextInt(10000)
//    })
//
//    val consumerTransactions = (0 to 100000).map(_ => new ConsumerTransaction {
//      override def transactionID: Long = scala.util.Random.nextLong()
//
//      override def name: String = rand.nextInt(10000).toString
//
//      override def stream: String = "1"
//
//      override def partition: Int = rand.nextInt(10000)
//    })
//
//
//    val resultInFuture = client.putTransactions(producerTransactions, Seq())
//    println(Await.ready(resultInFuture))
//
//    // val data = (0 to 1000000) map (_ => rand.nextString(10).getBytes().toSeq)
//  }

}
