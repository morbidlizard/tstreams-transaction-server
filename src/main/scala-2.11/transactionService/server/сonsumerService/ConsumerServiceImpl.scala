package transactionService.server.сonsumerService


import com.sleepycat.je._
import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.сonsumerService.ConsumerServiceImpl._
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.rpc.ConsumerService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl


trait ConsumerServiceImpl extends ConsumerService[TwitterFuture]
  with Authenticable
  with CheckpointTTL
{
  override def getConsumerState(token: Int, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    authenticate(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong
      val keyEntry = Key(name, streamNameToLong, partition).toDatabaseEntry
      val consumerTransactionEntry = new DatabaseEntry()
      val result: Long = if (database.get(transactionDB,keyEntry, consumerTransactionEntry,LockMode.DEFAULT) == OperationStatus.SUCCESS)
        ConsumerTransaction.entryToObject(consumerTransactionEntry).transactionId else -1L
      transactionDB.commit()
      result
    }

  override def setConsumerState(token: Int, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    authenticate(token) {
      val transactionDB = environment.beginTransaction(null, null)
      val streamNameToLong = getStreamDatabaseObject(stream).streamNameToLong
      val result = ConsumerTransactionKey(Key(name,streamNameToLong,partition), ConsumerTransaction(transaction))
        .put(database,transactionDB,Put.OVERWRITE,new WriteOptions()) != null
      if (result) transactionDB.commit() else transactionDB.abort()
      result
    }
}

object ConsumerServiceImpl {

  val environment = TransactionMetaServiceImpl.environment
  val database = {
    val dbConfig = new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    val storeName = configProperties.DB.ConsumerStoreName
    environment.openDatabase(null, storeName, dbConfig)
  }

  def close(): Unit = {
    database.close()
    environment.close()
  }
}
