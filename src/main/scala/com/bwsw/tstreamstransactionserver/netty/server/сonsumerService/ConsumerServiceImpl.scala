package com.bwsw.tstreamstransactionserver.netty.server.сonsumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL}
import com.bwsw.tstreamstransactionserver.options.StorageOptions
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import transactionService.rpc.ConsumerService

import scala.concurrent.{Future => ScalaFuture, _}

trait ConsumerServiceImpl extends Authenticable with CheckpointTTL {
  val executionContext: ServerExecutionContext
  val storageOpts: StorageOptions

  private val logger = LoggerFactory.getLogger(this.getClass)

  val consumerEnvironment: Environment
  lazy val consumerDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    consumerEnvironment.openDatabase(null, storageOpts.consumerStorageName, dbConfig)
  }

  def getConsumerState(token: Int, name: String, stream: String, partition: Int): ScalaFuture[Long] =
    authenticate(token) {
      val transactionDB = consumerEnvironment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong
      val keyEntry = Key(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long =
        if (consumerDatabase.get(transactionDB, keyEntry, consumerTransactionEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
          ConsumerTransactionWithoutKey.entryToObject(consumerTransactionEntry).transactionId
        else -1L

      transactionDB.commit()
      result
    }(executionContext.berkeleyReadContext)


  private final def transiteConsumerTranasctionToNewState(commitLogTransactions: Seq[ConsumerTransactionKey]): ConsumerTransactionKey = {
    commitLogTransactions.sortBy(_.timestamp).last
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionKey]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def setConsumerStates(consumerTransactions: Seq[ConsumerTransactionKey], parentBerkeleyTxn: com.sleepycat.je.Transaction): Boolean =
  {
    val nestedBerkeleyTxn = consumerEnvironment.beginTransaction(parentBerkeleyTxn, new TransactionConfig())

    groupProducerTransactions(consumerTransactions) foreach {case (key, txns) =>
      val theLastStateTransaction = transiteConsumerTranasctionToNewState(txns)
      val binaryKey = key.toDatabaseEntry
      consumerDatabase.put(nestedBerkeleyTxn, binaryKey, theLastStateTransaction.consumerTransaction.toDatabaseEntry)
    }

    scala.util.Try(nestedBerkeleyTxn.commit()) match {
      case scala.util.Success(_) => true
      case scala.util.Failure(_) => false
    }
  }

  def closeConsumerDatabase() = Option(consumerDatabase.close())
}