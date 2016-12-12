package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.twitter.util.{Await, Future}
import transactionService.rpc.TransactionStates
import transactionZookeeperService.TransactionZooKeeperClient

class TransactionLifeCycleWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = new TransactionZooKeeperClient
    val data = createTransactionData(dataSize)

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      val openedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
      val closedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      val t = time(Await.result(Future.collect(Seq(client.putTransactions(Seq(openedProducerTransaction), Seq()),
        client.putTransactionData(openedProducerTransaction, data, (txnCount - 1) * dataSize),
        client.putTransactions(Seq(closedProducerTransaction), Seq())))))
      (x, t)
    })

    println(s"Write to file $filename")
    writeTransactionsLifeCycleAndTime(filename, result)
  }
}
