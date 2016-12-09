package benchmark

import com.twitter.util.{Await, Future}
import transactionService.rpc.TransactionStates
import transactionZookeeperService.TransactionZooKeeperClient

class ClientWriterToOneTransaction(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  private val rand = new scala.util.Random()

  def run(txnCount: Int, dataSize: Int) {
    val filename = rand.nextInt(100) + s"СlientTo${partition}PartitionOneServerBenchmark.csv"
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

class ClientWriterToMultipleTransactions(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  private val rand = new scala.util.Random()

  def run(txnCount: Int) {
    val filename = rand.nextInt(100) + s"Сlient${txnCount}TransactionsOneServerBenchmark.csv"
    val client = new TransactionZooKeeperClient

    var globalProgress = 1
    val result = (1 to txnCount).map(x => {
      val localProgress = (x.toDouble / txnCount * 100).round
      if (globalProgress == localProgress) {
        println(localProgress + "%")
        globalProgress += 1
      }

      val openedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Opened)
      val closedProducerTransaction = createTransaction(streamName, partition, TransactionStates.Checkpointed, openedProducerTransaction.transactionID)
      val t1 = time(Await.result(client.putTransactions(Seq(openedProducerTransaction), Seq())))
      (x, t1, time(Await.result(client.putTransactions(Seq(closedProducerTransaction), Seq()))))
    })

    println(s"Write to file $filename")
    writeTransactionsAndTime(filename, result)
  }
}

class ClientWriterTransactionsLifeCycle(streamName: String, partition: Int = 1) extends TransactionCreator with CsvWriter with TimeMeasure {
  private val rand = new scala.util.Random()

  def run(txnCount: Int, dataSize: Int) {
    val filename = rand.nextInt(100) + s"Сlient${txnCount}TransactionsLifeCycleOneServerBenchmark.csv"
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