package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.HasEnvironment
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


trait ConsumerServiceImpl
  extends ConsumerTransactionStateNotifier
{
  val executionContext: ServerExecutionContext
  val rocksMetaServiceDB: RocksDBALL

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val consumerDatabase = rocksMetaServiceDB.getDatabase(HasEnvironment.CONSUMER_STORE)

  def getConsumerState(name: String, stream: String, partition: Int): Long = {
    val streamNameAsLong = getMostRecentStream(stream).id
    val consumerTransactionKey = ConsumerTransactionKey(name, streamNameAsLong, partition).toByteArray
    val consumerTransactionValue = Option(consumerDatabase.get(consumerTransactionKey))
    consumerTransactionValue.map(bytes =>
      ConsumerTransactionValue.fromByteArray(bytes).transactionId
    ).getOrElse {
      if (logger.isDebugEnabled()) logger.debug(s"There is no checkpointed consumer transaction on stream $name, partition $partition with name: $name. Returning -1L")
      -1L
    }
  }


  private final def transitConsumerTransactionToNewState(commitLogTransactions: Seq[ConsumerTransactionRecord]): ConsumerTransactionRecord = {
    commitLogTransactions.maxBy(_.timestamp)
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord], batch: Batch): ListBuffer[Unit => Unit] = {
    if (logger.isDebugEnabled()) logger.debug(s"Trying to commit consumer transactions: $consumerTransactions")
    val notifications = new ListBuffer[Unit => Unit]()
    groupProducerTransactions(consumerTransactions) foreach { case (key, txns) =>
      val theLastStateTransaction = transitConsumerTransactionToNewState(txns)
      val consumerTransactionValueBinary = theLastStateTransaction.consumerTransaction.toByteArray
      val consumerTransactionKeyBinary = key.toByteArray
      batch.put(HasEnvironment.CONSUMER_STORE, consumerTransactionKeyBinary, consumerTransactionValueBinary)
      if (areThereAnyConsumerNotifies) notifications += tryCompleteConsumerNotify(theLastStateTransaction)
    }
    notifications
  }
}