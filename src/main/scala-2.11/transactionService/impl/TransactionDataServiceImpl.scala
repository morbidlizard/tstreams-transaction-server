package transactionService.impl

import java.nio.ByteBuffer

import com.twitter.util.Future
import transactionService.rpc.TransactionDataService

class TransactionDataServiceImpl extends TransactionDataService[Future] {
  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): Future[Boolean] = ???

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[ByteBuffer]] = ???
}
