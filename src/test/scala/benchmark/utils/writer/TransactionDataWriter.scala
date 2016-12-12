package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.twitter.util.Await
import transactionService.rpc.TransactionStates
import transactionZookeeperService.TransactionZooKeeperClient

class TransactionDataWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = new TransactionZooKeeperClient

    var producerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
    println("Open a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()))) + " ms")

    val data = createTransactionData(dataSize)

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      x -> time(Await.result(client.putTransactionData(producerTransaction, data, (txnCount - 1) * dataSize)))
    })

    println(s"Write to file $filename")
    writeDataTransactionsAndTime(filename, result)

    producerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, producerTransaction.transactionID)
    println("Close a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()))) + " ms")
  }
}



