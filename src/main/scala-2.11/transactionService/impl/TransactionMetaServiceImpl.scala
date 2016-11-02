package transactionService.impl

import com.twitter.util.Future
import org.rocksdb.{WriteBatch, WriteOptions}
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
    val familyHandler = rocksDB.getFamilyHandler(key)

    val batch = new WriteBatch()
    batch.put(familyHandler, TransactionTable.interval, interval)
    batch.put(familyHandler, TransactionTable.state, state.value)
    batch.put(familyHandler, TransactionTable.quantity, quantity)
    batch.put(familyHandler, TransactionTable.timestamp, timestamp)

    val client = rocksDB.client
    client.write(new WriteOptions(), batch)


    batch.close()
    rocksDB.close()
    true
  }

  def putTransactions(token: String, transactions: Seq[Transaction]): Future[Boolean] = {
    val requests = Future.collect(transactions map (transaction => putTransaction(token, transaction)))
    requests flatMap (request => Future.value(request.forall(_ == true)))
  }


  def delTransaction(token: String, stream: String, partition: Int, interval: Long, transaction: Long): Future[Boolean] = Future {
    val rocksDB = new MyRocksDbConnection

    val key = s"$stream $partition $transaction"

    val batch = new WriteBatch()
    batch.put(familyHandler, TransactionTable.interval, interval)
    batch.put(familyHandler, TransactionTable.state, state.value)
    batch.put(familyHandler, TransactionTable.quantity, quantity)
    batch.put(familyHandler, TransactionTable.timestamp, timestamp)




    ???
  }

  def scanTransactions(token: String, stream: String, partition: Int, interval: Long): Future[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int, interval: Long): Future[Int] = ???
}
