package transactionService.impl

import java.nio.ByteBuffer

import com.twitter.util.Future
import transactionService.rpc.TransactionDataService

trait TransactionDataServiceImpl extends TransactionDataService[Future] {
  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): Future[Boolean] = ???

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[ByteBuffer]] = ???
}

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