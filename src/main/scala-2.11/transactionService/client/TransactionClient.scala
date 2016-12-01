package transactionService.client

import java.nio.ByteBuffer

import authService.AuthClient

import scala.concurrent.{Future => ScalaFuture}
import com.twitter.util.{Await, Future => TwitterFuture}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Stream, Transaction, TransactionService, TransactionStates}
import com.twitter.conversions.time._

class TransactionClient(serverIPAddress: String)/*(implicit val threadPool: transactionService.Context)*/ extends TransactionService[TwitterFuture] {
  private val client = Thrift.client
    .withSessionQualifier.noFailFast
    .withSessionQualifier.noFailureAccrual


  private def getInterface = {
    val interface = client.newServiceIface[TransactionService.ServiceIface](serverIPAddress, "transaction")
    interface.copy(
      putStream = interface.putStream,
      doesStreamExist = interface.doesStreamExist,
      getStream = interface.getStream,
      delStream = interface.delStream,
      putTransaction = interface.putTransaction,
      putTransactions = interface.putTransactions,
      scanTransactions = interface.scanTransactions,
      putTransactionData = interface.putTransactionData,
      getTransactionData = interface.getTransactionData,
      setConsumerState = interface.setConsumerState,
      getConsumerState = interface.getConsumerState
    )
  }

  private val request = Thrift.client.newMethodIface(getInterface)

  //Stream API
  override def putStream(token: String, stream: String, partitions: Int, description: Option[String], ttl: Int): TwitterFuture[Boolean] = {
    request.putStream(token, stream, partitions, description, ttl)
  }
  override def doesStreamExist(token: String, stream: String): TwitterFuture[Boolean] = request.doesStreamExist(token, stream)
  override def getStream(token: String, stream: String): TwitterFuture[Stream]  = request.getStream(token, stream)
  override def delStream(token: String, stream: String): TwitterFuture[Boolean] = request.delStream(token, stream)

  //TransactionMeta API
  override def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = {
    request.putTransaction(token, transaction)
  }
  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = {
    request.putTransactions(token, transactions)
  }
  override def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = request.scanTransactions(token, stream, partition)

  //TransactionData API
  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer]): TwitterFuture[Boolean] =
    request.putTransactionData(token, stream, partition, transaction, data)
  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
    request.getTransactionData(token,stream,partition,transaction,from,to)

  //Consumer API
  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    request.setConsumerState(token,name,stream,partition,transaction)
  override def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    request.getConsumerState(token,name,stream,partition)
}