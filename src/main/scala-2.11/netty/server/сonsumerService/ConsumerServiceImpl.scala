package netty.server.сonsumerService

import com.sleepycat.je._

//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture, _}
import netty.server.сonsumerService.ConsumerServiceImpl._
import netty.server.{Authenticable, CheckpointTTL}
import netty.server.transactionMetaService.TransactionMetaServiceImpl
import transactionService.rpc.ConsumerService


trait ConsumerServiceImpl extends ConsumerService[ScalaFuture]
  with Authenticable
  with CheckpointTTL
{

  override def getConsumerState(token: Int, name: String, stream: String, partition: Int): ScalaFuture[Long] =
    authenticate(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong
      val keyEntry = Key(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long = if (database.get(transactionDB,keyEntry, consumerTransactionEntry,LockMode.DEFAULT) == OperationStatus.SUCCESS)
        ConsumerTransaction.entryToObject(consumerTransactionEntry).transactionId else -1L
      transactionDB.commit()
      result
    }(netty.Context.berkeleyReadPool.getContext)

  override def setConsumerState(token: Int, name: String, stream: String, partition: Int, transaction: Long): ScalaFuture[Boolean] =
    authenticateFutureBody(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong

      val promise = Promise[Boolean]
      val result = promise success (ConsumerTransactionKey(Key(name, streamNameToLong, partition), ConsumerTransaction(transaction))
        .put(database, transactionDB, Put.OVERWRITE, new WriteOptions()) != null)
      result.future.map { isOkay =>
        if (isOkay) transactionDB.commit() else transactionDB.abort()
        isOkay
      }(netty.Context.berkeleyWritePool.getContext)
    }(netty.Context.berkeleyWritePool.getContext)
}

object ConsumerServiceImpl {

  val environment = TransactionMetaServiceImpl.environment
  val database = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(false)
    val storeName = configProperties.DB.ConsumerStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }

  def close(): Unit = {
    database.close()
    environment.close()
  }
}
