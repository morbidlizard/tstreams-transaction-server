package netty.server.сonsumerService

import com.sleepycat.je._
import configProperties.ServerConfig

import scala.concurrent.{Future => ScalaFuture, _}
import netty.server.{Authenticable, CheckpointTTL}
import transactionService.rpc.ConsumerService


trait ConsumerServiceImpl extends ConsumerService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  val config: ServerConfig

  val consumerEnvironment: Environment
  lazy val consumerDatabase = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
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
        ConsumerTransaction.entryToObject(consumerTransactionEntry).transactionId else -1L
      transactionDB.commit()
      result
    }(config.berkeleyReadPool.getContext)

  override def setConsumerState(token: Int, name: String, stream: String, partition: Int, transaction: Long): ScalaFuture[Boolean] =
    authenticateFutureBody(token) {
      val transactionDB = consumerEnvironment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong

      val promise = Promise[Boolean]
      val result = promise success (ConsumerTransactionKey(Key(name, streamNameToLong, partition), ConsumerTransaction(transaction))
        .put(consumerDatabase, transactionDB, Put.OVERWRITE, new WriteOptions()) != null)
      result.future.flatMap { isOkay =>
        if (isOkay) transactionDB.commit() else transactionDB.abort()
        Promise.successful(isOkay).future
      }(config.berkeleyWritePool.getContext)
    }

  def closeConsumerDatabase() = consumerDatabase.close()
}