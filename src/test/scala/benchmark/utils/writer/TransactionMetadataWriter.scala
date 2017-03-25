package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.concurrent.Await
import scala.concurrent.duration._

class TransactionMetadataWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, filename: String) {
    val client = new ClientBuilder()
        .withConnectionOptions(ConnectionOptions(requestTimeoutMs = 100))
      .build()

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      val openedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
      (x, {
        time(Await.result(client.putTransactions(Seq(openedProducerTransaction), Seq()), 5.seconds))
      })
    })

    println(s"Write to file $filename")
    writeMetadataTransactionsAndTime(filename, result)

    client.shutdown()
  }
}
