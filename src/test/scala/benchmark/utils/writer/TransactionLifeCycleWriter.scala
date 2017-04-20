package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class TransactionLifeCycleWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(requestTimeoutMs = 200))
      .build()
    val data = createTransactionData(dataSize)

    implicit val context = ExecutionContext.Implicits.global

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {

      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      val openedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
      val closedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      val t =
        time(Await.result(
          Future.sequence(Seq(
            client.putTransactionData(openedProducerTransaction.stream, openedProducerTransaction.partition, openedProducerTransaction.transactionID, data, (txnCount - 1) * dataSize),
            client.putTransactionData(openedProducerTransaction.stream, openedProducerTransaction.partition, openedProducerTransaction.transactionID, data, (txnCount - 1) * dataSize),
            client.putTransactionData(openedProducerTransaction.stream, openedProducerTransaction.partition, openedProducerTransaction.transactionID, data, (txnCount - 1) * dataSize))), 10.seconds)
        )
      (x, t)
    })

    println(s"Write to file $filename")
    writeTransactionsLifeCycleAndTime(filename, result)

    client.shutdown()
  }
}
