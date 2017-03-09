package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, StreamCache}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import transactionService.rpc.ConsumerService

import scala.concurrent.{Future => ScalaFuture, _}

trait ConsumerServiceImpl extends Authenticable with StreamCache {
  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)

  val environment: Environment
  val consumerDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    environment.openDatabase(null, storageOpts.consumerStorageName, dbConfig)
  }

  def getConsumerState(token: Int, name: String, stream: String, partition: Int): ScalaFuture[Long] =
    authenticate(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getStreamFromOldestToNewest(stream).last.streamNameToLong
      val keyEntry = Key(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long =
        if (consumerDatabase.get(transactionDB, keyEntry, consumerTransactionEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
          ConsumerTransactionWithoutKey.entryToObject(consumerTransactionEntry).transactionId
        else -1L

      transactionDB.commit()
      result
    }(executionContext.berkeleyReadContext)


  private final def transiteConsumerTransactionToNewState(commitLogTransactions: Seq[ConsumerTransactionKey]): ConsumerTransactionKey = {
    commitLogTransactions.sortBy(_.timestamp).last
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionKey]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def setConsumerStates(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn: com.sleepycat.je.Transaction): Unit =
  {
    groupProducerTransactions(consumerTransactions) foreach {case (key, txns) =>
      val theLastStateTransaction = transiteConsumerTransactionToNewState(txns)
      val binaryKey = key.toDatabaseEntry
      consumerDatabase.put(parentBerkeleyTxn, binaryKey, theLastStateTransaction.consumerTransaction.toDatabaseEntry)
    }
  }
  def closeConsumerDatabase() = scala.util.Try(consumerDatabase.close())
}