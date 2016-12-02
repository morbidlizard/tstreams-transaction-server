package benchmark

import java.io.{File, PrintWriter}

import com.twitter.util.{Await, Time}
import transactionZookeeperService.TransactionZooKeeperClient

object OneClientOneServerTest {
  def main(args: Array[String]) {
    Installation.clearDB()
    Installation.startAuthServer()
    Installation.startTransactionServer()
    TestClient.main(Array("100"))
    System.exit(0)
  }
}

object TestClient {

  import transactionService.rpc.{ProducerTransaction, TransactionStates}

  private val streamName = "one-partition"
  private val rand = new scala.util.Random()
  private val dataSize = 10000

  def main(args: Array[String]) {
    val client = new TransactionZooKeeperClient
    Await.result(client.putStream(streamName, 1, None, 5))

    val producerTransactions = createProducerTransactions(1)

    Await.result(client.putTransactions(producerTransactions, Seq()))

    val data = createTransactionData(dataSize)

    val a = (1 to args(0).toInt).map(x => x -> time(Await.result(client.putTransactionData(producerTransactions.head, data))))

    writeAsCSV(a, dataSize)

    Await.result(client.delStream(streamName))
  }

  def createProducerTransactions(count: Int) = {
    (0 until count).map(_ => createOpenedTransaction(streamName))
  }

  def createOpenedTransaction(streamName: String) = {
    new ProducerTransaction {
      override val transactionID: Long = rand.nextLong()

      override val state: TransactionStates = TransactionStates.Opened

      override val stream: String = streamName

      override val timestamp: Long = Time.epoch.inNanoseconds

      override val quantity: Int = -1

      override val partition: Int = rand.nextInt(1)
    }
  }

  def createTransactionData(count: Int) = {
    (0 until count) map (_ => "data".getBytes())
  }

  def writeAsCSV(data: IndexedSeq[(Int, Long)], dataSize: Int) = {
    val file = new File("oneClientOneServerBenchmark.csv")
    val bw = new PrintWriter(file)
    bw.write("Number of records, Time (ms)\n")
    data.foreach(x => bw.write(x._1 * dataSize + ", " + x._2 + "\n"))
    bw.close()
  }

  def time(block: => Unit) = {
    val t0 = System.currentTimeMillis()
    block
    val t1 = System.currentTimeMillis()

    t1 - t0
  }
}