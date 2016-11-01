package transactionService.impl

import com.twitter.util.Future
import transactionService.rpc.{Transaction, TransactionMetaService}

class TransactionMetaServiceImpl extends TransactionMetaService[Future] {
  def putTransaction(token: String, transactions: Seq[Transaction]): Future[Boolean] = ???

  def delTransaction(token: String, stream: String, partition: Int, interval: Long, transaction: Long): Future[Boolean] = ???

  def scanTransactions(token: String, stream: String, partition: Int, interval: Long): Future[Seq[Transaction]] = ???

  def scanTransactionsCRC32(token: String, stream: String, partition: Int, interval: Long): Future[Int] = ???
}
