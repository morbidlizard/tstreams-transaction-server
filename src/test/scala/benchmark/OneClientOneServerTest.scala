package benchmark

object OneClientOneTxnOneServerTest extends Installer {
  private val streamName = "stream"
  private val partitions = 1
  private val txnCount = 1000000
  private val dataSize = 1

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    createStream(streamName, partitions)
    new ClientWriterToOneTransaction(streamName).run(txnCount, dataSize)
    deleteStream(streamName)
    System.exit(0)
  }
}

object OneClientMultipleTxnOneServerTest extends Installer {
  private val streamName = "stream"
  private val partitions = 1
  private val txnCount = 1000000

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    createStream(streamName, partitions)
    new ClientWriterToMultipleTransactions(streamName).run(txnCount)
    deleteStream(streamName)
    System.exit(0)
  }
}

object OneClientMultipleTxnLifeCycleOneServerTest extends Installer {
  private val streamName = "stream"
  private val partitions = 1
  private val txnCount = 1000000
  private val dataSize = 100

  def main(args: Array[String]) {
    clearDB()
    startAuthServer()
    startTransactionServer()
    createStream(streamName, partitions)
    new ClientWriterTransactionsLifeCycle(streamName).run(txnCount, dataSize)
    deleteStream(streamName)
    System.exit(0)
  }
}
