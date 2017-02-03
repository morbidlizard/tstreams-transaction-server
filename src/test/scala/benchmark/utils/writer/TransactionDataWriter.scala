package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import transactionService.rpc.TransactionStates

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionDataWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = new ClientBuilder().build()

    var producerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
    println("Open a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()),10.seconds)) + " ms")

    val data = createTransactionData(dataSize)

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      x -> time(Await.result(client.putTransactionData(producerTransaction, data, (txnCount - 1) * dataSize), 10.seconds))
    })

    println(s"Write to file $filename")
    writeDataTransactionsAndTime(filename, result)

    producerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, producerTransaction.transactionID)
    println("Close a txn: " + time(Await.result(client.putTransactions(Seq(producerTransaction), Seq()),10.seconds)) + " ms")

    client.shutdown()
  }
}



