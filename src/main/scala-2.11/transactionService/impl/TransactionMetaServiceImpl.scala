package transactionService.impl

import com.sleepycat.persist.model.{Entity, KeyField, Persistent, PrimaryKey}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Future, Try}
import org.rocksdb.{RocksDBException, WriteBatch, WriteOptions}
import transactionService.impl.db._
import transactionService.impl.`implicit`.Implicits._
import transactionService.rpc.{Transaction, TransactionMetaService}

trait TransactionMetaServiceImpl extends TransactionMetaService[Future] {
  def putTransaction(token: String, transaction: Transaction): Future[Boolean] = Future {
    val rocksDB = new MyRocksDbConnection

    val stream = transaction._1
    val partition = transaction._2
    val interval = transaction._3
    val transactionId = transaction._4
    val state = transaction._5
    val quantity = transaction._6
    val timestamp = transaction._7


    val key = s"$stream $partition $transactionId"
    val familyHandler = rocksDB.getOrCreateFamilyHandler(key)

    val batch = new WriteBatch()
    batch.put(familyHandler, s"${TransactionTable.interval} $interval", Array.emptyByteArray)
    batch.put(familyHandler, TransactionTable.state, state.value)
    batch.put(familyHandler, TransactionTable.quantity, quantity)
    batch.put(familyHandler, TransactionTable.timestamp, timestamp)

    val client = rocksDB.client
    client.write(new WriteOptions(), batch)


    batch.close()
    rocksDB.close()
    true
  }

  def putTransactions(token: String, transactions: Seq[Transaction]): Future[Boolean] = Future {
    val rocksDB = new MyRocksDbConnection
    val batch = new WriteBatch()

    transactions foreach  { transaction =>
      val stream = transaction._1
      val partition = transaction._2
      val interval = transaction._3
      val transactionId = transaction._4
      val state = transaction._5
      val quantity = transaction._6
      val timestamp = transaction._7

      val key = s"$stream $partition $transactionId"
      val familyHandler = rocksDB.getOrCreateFamilyHandler(key)

      batch.put(familyHandler, s"${TransactionTable.interval} $interval", Array.emptyByteArray)
      batch.put(familyHandler, TransactionTable.state, state.value)
      batch.put(familyHandler, TransactionTable.quantity, quantity)
      batch.put(familyHandler, TransactionTable.timestamp, timestamp)
    }

    val client = rocksDB.client
    client.write(new WriteOptions(), batch)

    batch.close()
    rocksDB.close()
    true
  }

  def delTransaction(token: String, stream: String, partition: Int, interval: Long, transaction: Long): Future[Boolean] = Future {
    val rocksDB = new MyRocksDbConnection
    val client = rocksDB.client

    val key = s"$stream $partition $transaction"
    val familyHandler = rocksDB.getCreateFamilyHandlerOpt(key)

    val res = familyHandler match {
      case Some(handler) => {
        val tryToDelete = Try(client.remove(handler, s"${TransactionTable.interval} $interval"))
        if (tryToDelete.isReturn) true else false
      }
      case None => false
    }
    rocksDB.close()
    res
  }

  def scanTransactions(token: String, stream: String, partition: Int, interval: Long): Future[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int, interval: Long): Future[Int] = ???
}

object TransactionMetaServiceImpl {
  @Entity
  class Transaction extends transactionService.rpc.Transaction {
    @PrimaryKey var myPrimaryKey: MyKey = _
    private var stateDB: Int = _
    private var timestampDB: java.lang.Long = _
    private var intervalDB: java.lang.Long = _
    private var quantityDB: Int = _

    def this(transactionID: java.lang.Long,
             state: Int,
             stream: Int,
             timestamp: java.lang.Long,
             interval: java.lang.Long,
             quantity: Int,
             partition: Int) {
      this()
      this.stateDB = state
      this.timestampDB = timestamp
      this.intervalDB = interval
      this.quantityDB = quantity
      this.myPrimaryKey = new MyKey(stream, partition,transactionID)
    }

    override def toString: String = {s"$myPrimaryKey"}
  }

  @Persistent
  class MyKey {
    @KeyField(1) var stream: Int = _
    @KeyField(2) var partition: Int = _
    @KeyField(3) var transactionID: java.lang.Long = _
    def this(stream: Int, partition:Int, transactionID: java.lang.Long) = {
      this()
      this.stream = stream
      this.partition = partition
      this.transactionID = transactionID
    }

    override def toString: String = s"$stream $partition $transactionID"
  }
}
