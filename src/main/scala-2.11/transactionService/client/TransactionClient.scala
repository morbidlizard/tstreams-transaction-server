package transactionService.client

import java.nio.ByteBuffer

import com.twitter.util.{Future => TwitterFuture}
import com.twitter.finagle.{Resolver, Thrift}
import filter.Filter
import transactionService.rpc.{Stream, Transaction, TransactionService}

class TransactionClient(serverIPAddress: String, authTimeoutConnection: Int, authTimeoutExponentialBetweenRetries: Int) extends TransactionService[TwitterFuture] {
  private val client = Thrift.client
    .withSessionQualifier.noFailFast
    .withSessionQualifier.noFailureAccrual

  private def timeoutFilter[Req, Rep] = Filter
    .filter[Req, Rep](authTimeoutConnection, authTimeoutExponentialBetweenRetries, Filter.retryConditionToConnectToMaster)

  private def getInterface = {
    val (name, label) = Resolver.evalLabeled(serverIPAddress)
    val interface = client.newServiceIface[TransactionService.ServiceIface](name, label)
    interface.copy(
      putStream = timeoutFilter andThen interface.putStream,
      doesStreamExist = timeoutFilter andThen interface.doesStreamExist,
      getStream = timeoutFilter andThen interface.getStream,
      delStream = timeoutFilter andThen interface.delStream,
      putTransaction = timeoutFilter andThen interface.putTransaction,
      putTransactions = timeoutFilter andThen interface.putTransactions,
      scanTransactions = timeoutFilter andThen interface.scanTransactions,
      putTransactionData = timeoutFilter andThen interface.putTransactionData,
      getTransactionData = timeoutFilter andThen interface.getTransactionData,
      setConsumerState = timeoutFilter andThen interface.setConsumerState,
      getConsumerState = timeoutFilter andThen interface.getConsumerState,
      authenticate = timeoutFilter andThen interface.authenticate,
      isValid = timeoutFilter andThen interface.isValid
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
  override def scanTransactions(token: String, stream: String, partition: Int, from: Long, to: Long): TwitterFuture[Seq[Transaction]] = request.scanTransactions(token, stream, partition, from, to)

  //TransactionData API
  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): TwitterFuture[Boolean] =
    request.putTransactionData(token, stream, partition, transaction, data, from)
  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
    request.getTransactionData(token,stream,partition,transaction,from,to)

  //Consumer API
  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    request.setConsumerState(token,name,stream,partition,transaction)
  override def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    request.getConsumerState(token,name,stream,partition)

  //Auth API
  override def isValid(token: String): TwitterFuture[Boolean] =
    request.isValid(token)
  override def authenticate(login: String, password: String): TwitterFuture[String] =
    request.authenticate(login, password)
}