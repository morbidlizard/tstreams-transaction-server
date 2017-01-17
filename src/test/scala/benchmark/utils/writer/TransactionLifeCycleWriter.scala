package benchmark.utils.writer

import benchmark.utils.{CsvWriter, TimeMeasure, TransactionCreator}
import transactionService.rpc.TransactionStates

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class TransactionLifeCycleWriter(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  def run(txnCount: Int, dataSize: Int, filename: String) {
    val client = new netty.client.Client()
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
      val t = time(Await.result(
        Future.sequence(Seq(
          client.putTransaction(openedProducerTransaction),
          client.putTransactionData(openedProducerTransaction, data, (txnCount - 1) * dataSize),
          client.putTransaction(closedProducerTransaction))), 10.seconds))
      (x, t)
    })

    println(s"Write to file $filename")
    writeTransactionsLifeCycleAndTime(filename, result)
  }
}
