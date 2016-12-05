package benchmark

import com.twitter.util.Time
import transactionService.rpc.{TransactionStates, ProducerTransaction}

trait TransactionCreator {
  private val rand = new scala.util.Random()

  def createProducerTransactions(streamName: String, _type: TransactionStates, count: Int) = {
    (0 until count).map(_ => createTransaction(streamName, _type))
  }

  private def createTransaction(streamName: String, _type: TransactionStates) = {
    new ProducerTransaction {
      override val transactionID: Long = rand.nextLong()

      override val state: TransactionStates = _type

      override val stream: String = streamName

      override val timestamp: Long = Time.epoch.inNanoseconds

      override val quantity: Int = -1

      override val partition: Int = rand.nextInt(1)
    }
  }

  def createTransactionData(count: Int) = {
    (0 until count) map (_ => "data".getBytes)
  }
}
