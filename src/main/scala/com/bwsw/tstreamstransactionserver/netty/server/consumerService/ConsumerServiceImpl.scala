package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je._
import org.slf4j.LoggerFactory

import scala.concurrent.{Future => ScalaFuture}

trait ConsumerServiceImpl extends Authenticable with StreamCache {
  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)

  val environment: Environment
  val consumerDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    environment.openDatabase(null, "ConsumerStore"/*storageOpts.consumerStorageName*/, dbConfig)
  }

  def getConsumerState(name: String, stream: String, partition: Int): ScalaFuture[Long] =
    ScalaFuture {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getMostRecentStream(stream).streamNameToLong
      val keyEntry = ConsumerTransactionKey(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long =
        if (consumerDatabase.get(transactionDB, keyEntry, consumerTransactionEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
          ConsumerTransactionValue.entryToObject(consumerTransactionEntry).transactionId
        else {
          if (logger.isDebugEnabled()) logger.debug(s"There is no checkpointed consumer transaction on stream $name, partition $partition with name: $name. Returning -1")
          -1L
        }

      transactionDB.commit()
      result
    }(executionContext.berkeleyReadContext)


  private final def transitConsumerTransactionToNewState(commitLogTransactions: Seq[ConsumerTransactionRecord]): ConsumerTransactionRecord = {
    commitLogTransactions.maxBy(_.timestamp)
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit =
  {
    if (logger.isDebugEnabled()) logger.debug(s"Trying to commit consumer transactions: $consumerTransactions")
    groupProducerTransactions(consumerTransactions) foreach {case (key, txns) =>
      val theLastStateTransaction = transitConsumerTransactionToNewState(txns)
      val binaryKey = key.toDatabaseEntry
      consumerDatabase.put(parentBerkeleyTxn, binaryKey, theLastStateTransaction.consumerTransaction.toDatabaseEntry)
    }
  }
  def closeConsumerDatabase() = scala.util.Try(consumerDatabase.close())
}