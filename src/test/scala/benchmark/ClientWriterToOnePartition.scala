package benchmark

import com.twitter.util.Await
import transactionService.rpc.TransactionStates
import transactionZookeeperService.TransactionZooKeeperClient

object ClientWriterToOnePartition extends TransactionCreator with CsvWriter with TimeMeasure {
  private val rand = new scala.util.Random()
  private val streamName = "one-partition"

  def main(args: Array[String]) {
    val filename = rand.nextInt(1000) + "clientOneServerBenchmark.csv"
    val txnCount = args(0).toInt
    val dataSize = args(1).toInt
    val client = new TransactionZooKeeperClient
    Await.result(client.putStream(streamName, 1, None, 5))

    val producerTransactions = createProducerTransactions(streamName, TransactionStates.Opened, 1)

    Await.result(client.putTransactions(producerTransactions, Seq()))

    val data = createTransactionData(dataSize)

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      x -> time(Await.result(client.putTransactionData(producerTransactions.head, data, ???)))
    })

    writeTransactionsAndTime(filename, result)

    Await.result(client.delStream(streamName))
  }
}
