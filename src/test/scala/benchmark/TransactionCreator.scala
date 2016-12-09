package benchmark

import com.twitter.util.Time
import transactionService.rpc.{ProducerTransaction, TransactionStates}

import scala.collection.immutable.IndexedSeq

trait TransactionCreator {
  private val rand = new scala.util.Random()

  def createProducerTransactions(streamName: String, partition: Int, _type: TransactionStates, count: Int) = {
    (0 until count).map(_ => createTransaction(streamName, partition, _type))
  }

  def createTransaction(streamName: String, _partition: Int, _type: TransactionStates): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = rand.nextLong()

      override val state: TransactionStates = _type

      override val stream: String = streamName

      override val timestamp: Long = Time.epoch.inNanoseconds

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransaction(streamName: String, _partition: Int, _type: TransactionStates, id: Long): ProducerTransaction = {
    new ProducerTransaction {
      override val transactionID: Long = id

      override val state: TransactionStates = _type

      override val stream: String = streamName

      override val timestamp: Long = Time.epoch.inNanoseconds

      override val quantity: Int = -1

      override val partition: Int = _partition
    }
  }

  def createTransactionData(count: Int): IndexedSeq[Array[Byte]] = {
    (0 until count) map (_ => "data".getBytes)
  }
}
