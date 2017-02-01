package com.bwsw.tstreamstransactionserver.netty.server.сonsumerService

import com.sleepycat.je._
import com.bwsw.tstreamstransactionserver.configProperties.ServerConfig

import scala.concurrent.{Future => ScalaFuture, _}
import com.bwsw.tstreamstransactionserver.netty.server.{Authenticable, CheckpointTTL, Server}
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import transactionService.rpc.ConsumerService


trait ConsumerServiceImpl extends ConsumerService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  val config: ServerConfig

  PropertyConfigurator.configure("src/main/resources/logServer.properties")
  private val logger = LoggerFactory.getLogger(classOf[Server])

  val consumerEnvironment: Environment
  lazy val consumerDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = config.consumerStoreName
    consumerEnvironment.openDatabase(null, storeName, dbConfig)
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
    }(config.berkeleyReadPool.getContext)


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
          logger.debug(s"$stream $partition $transaction. Successfully setted consumer state.")
          transactionDB.commit()
        } else {
          logger.debug(s"$stream $partition $transaction. Consumer state isn't setted.")
          transactionDB.abort()
        }
        Promise.successful(isOkay).future
      }(config.berkeleyWritePool.getContext)
    }

  def closeConsumerDatabase() = Option(consumerDatabase.close())
}