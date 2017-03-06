package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContext
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.StorageOptions
import com.sleepycat.je._
import org.slf4j.LoggerFactory
import transactionService.rpc.ConsumerService

import scala.concurrent.{Future => ScalaFuture, _}

trait ConsumerServiceImpl extends ConsumerService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

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

  override def getConsumerState(token: Int, name: String, stream: String, partition: Int): ScalaFuture[Long] =
    authenticate(token) {
      val transactionDB = consumerEnvironment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong
      val keyEntry = Key(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long = if (consumerDatabase.get(transactionDB, keyEntry, consumerTransactionEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
        ConsumerTransaction.entryToObject(consumerTransactionEntry).transactionId
      else -1L
      transactionDB.commit()
      result
    }(executionContext.berkeleyReadContext)


  def setConsumerState(transactionDB: Transaction, name: String, stream: String, partition: Int, transaction: Long): Boolean = {
    val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong

    ConsumerTransactionKey(Key(name, streamNameToLong, partition), ConsumerTransaction(transaction))
      .put(consumerDatabase, transactionDB, Put.OVERWRITE, new WriteOptions()) != null
  }

  override def setConsumerState(token: Int, name: String, stream: String, partition: Int, transaction: Long): ScalaFuture[Boolean] =
    authenticateFutureBody(token) {
      val transactionDB = consumerEnvironment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong

      val promise = Promise[Boolean]
      val result = promise success (ConsumerTransactionKey(Key(name, streamNameToLong, partition), ConsumerTransaction(transaction))
        .put(consumerDatabase, transactionDB, Put.OVERWRITE, new WriteOptions()) != null)
      result.future.flatMap { isOkay =>
        if (isOkay) {
          logger.debug(s"$stream $partition $transaction. Successfully set consumer state.")
          transactionDB.commit()
        } else {
          logger.debug(s"$stream $partition $transaction. Consumer state hasn't been set.")
          transactionDB.abort()
        }
        Promise.successful(isOkay).future
      }(executionContext.berkeleyWriteContext)
    }

  def closeConsumerDatabase() = scala.util.Try(consumerDatabase.close())
}