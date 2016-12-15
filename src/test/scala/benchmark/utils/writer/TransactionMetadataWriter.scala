package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.twitter.util.Await
import transactionService.rpc.TransactionStates
import transactionZookeeperService.TransactionZooKeeperClient

class TransactionMetadataWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, filename: String) {
    //val client = new TransactionClient("localhost:8071", 5000, 5000)
    val client = new TransactionZooKeeperClient
    //val token = ""
    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      //val openedProducerTransaction = Transaction(Some(createTransaction(streamName, partition, TransactionStates.Opened)), None)
      val openedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
      //val closedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      (x, {
        time(Await.result(client.putTransactions(Seq(openedProducerTransaction), Seq())))
      })
    })

    println(s"Write to file $filename")
    writeMetadataTransactionsAndTime(filename, result)
  }
}
