package transactionService.impl

import java.io.File

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, KeyField, Persistent, PrimaryKey}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Try}
import org.rocksdb.{RocksDBException, WriteBatch, WriteOptions}
import transactionService.impl.db._
import transactionService.impl.`implicit`.Implicits._
import transactionService.rpc.{Transaction, TransactionMetaService, TransactionStates}

trait TransactionMetaServiceImpl extends TransactionMetaService[Future] {
  def putTransaction(token: String, transaction: Transaction): Future[Boolean] = ???
//    Future {
//    val rocksDB = new MyRocksDbConnection
//
//    val stream = transaction._1
//    val partition = transaction._2
//    val transactionId = transaction._3
//    val state = transaction._4
//    val quantity = transaction._5
//    val timestamp = transaction._6
//
//
//    val key = s"$stream $partition $transactionId"
//    val familyHandler = rocksDB.getOrCreateFamilyHandler(key)
//
//    val batch = new WriteBatch()
//    batch.put(familyHandler, TransactionTable.state, state.value)
//    batch.put(familyHandler, TransactionTable.quantity, quantity)
//    batch.put(familyHandler, TransactionTable.timestamp, timestamp)
//
//    val client = rocksDB.client
//    client.write(new WriteOptions(), batch)
//
//
//    batch.close()
//    rocksDB.close()
//    true
//  }

  def putTransactions(token: String, transactions: Seq[Transaction]): Future[Boolean] = Future {
    val environmentConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setLocking(true)
      .setTransactional(true)

    val storeConfig = new StoreConfig()
      .setAllowCreate(true)

    val directory = StreamServiceImpl.pathToDatabases
    val environment = new Environment(new File(directory), environmentConfig)
    import scala.collection.JavaConverters._
    environment.getDatabaseNames.asScala foreach println

    //val store = new EntityStore(environment, "EntityStore", storeConfig)

    true
  }

  def delTransaction(token: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = ???
//    Future {
//    val rocksDB = new MyRocksDbConnection
//    val client = rocksDB.client
//
//    val key = s"$stream $partition $transaction"
//    val familyHandler = rocksDB.getCreateFamilyHandlerOpt(key)
//
//    val res = familyHandler match {
//      case Some(handler) => {
//        val tryToDelete = Try(client.remove(handler, s"${TransactionTable.interval} $interval"))
//        if (tryToDelete.isReturn) true else false
//      }
//      case None => false
//    }
//    rocksDB.close()
//    res
//  }

  def scanTransactions(token: String, stream: String, partition: Int): Future[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int): Future[Int] = ???
}

object TransactionMetaServiceImpl {
//  @Entity
//  class Transaction extends transactionService.rpc.Transaction {
//    @PrimaryKey var key: TransactionKey = _
//    private var stateDB: Int = _
//    private var timestampDB: java.lang.Long = _
//    private var quantityDB: Int = _
//
//    def this(transactionID: java.lang.Long,
//             state: Int,
//             stream: String,
//             timestamp: java.lang.Long,
//             quantity: Int,
//             partition: Int) {
//      this()
//      this.stateDB = state
//      this.timestampDB = timestamp
//      this.quantityDB = quantity
//      this.key = new TransactionKey(stream, partition, transactionID)
//    }
//
//    override def transactionID: Long = key.transactionID
//    override def state: TransactionStates = TransactionStates(stateDB)
//    override def stream: String = key.stream
//    override def timestamp: Long = timestampDB
//    override def quantity: Int = quantityDB
//    override def partition: Int = key.partition
//
//    override def toString: String = {s"$key"}
//  }
//
//  @Persistent
//  class TransactionKey {
//    @KeyField(1) var stream: String = _
//    @KeyField(2) var partition: Int = _
//    @KeyField(3) var transactionID: java.lang.Long = _
//    def this(stream: String, partition:Int, transactionID: java.lang.Long) = {
//      this()
//      this.stream = stream
//      this.partition = partition
//      this.transactionID = transactionID
//    }
//
//    override def toString: String = s"$stream $partition $transactionID"
//  }
}
