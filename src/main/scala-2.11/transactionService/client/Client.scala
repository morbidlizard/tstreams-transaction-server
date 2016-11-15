package transactionService.client

import java.nio.ByteBuffer

import authService.ClientAuth
import authService.rpc.AuthService

import scala.concurrent.{Future => ScalaFuture}
import com.twitter.util.{Await, Duration, Monitor, Throw, Time, Try, Future => TwitterFuture}
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.finagle.service.{Backoff, RetryExceptionsFilter, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{CancelledConnectionException, FailedFastException, Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import transactionService.rpc.{ConsumerTransaction, ProducerTransaction, Stream, Transaction, TransactionService, TransactionStates}
import com.twitter.conversions.time._
import com.twitter.finagle.param.HighResTimer

import scala.collection.mutable.ArrayBuffer


class Client(login: String, password: String, serverIPAddress: String, authServerIPAddress: String)/*(implicit val threadPool: transactionService.Context)*/ extends TransactionService[TwitterFuture] {
//  private def getTransactionStreamAndPartition(transaction: Transaction): (String, Int) =
//    (transaction.producerTransaction,transaction.consumerTransaction) match {
//    case (Some(txn),_) => (txn.stream,txn.partition)
//    case (_,Some(txn)) => (txn.stream,txn.partition)
//    case _ => throw new IllegalArgumentException("Empty transaction isn't allowed!")
//  }

  private val authClient = new ClientAuth(authServerIPAddress)
  def authenticate(login: String, password: String) = authClient.authenticate(login,password)
  def isValid(token: String) = authClient.isValid(token)

  @volatile var token: String = Await.result(authenticate(login,password))
  def timeoutFilter[Req, Rep](duration: Duration) = {
    val timer = DefaultTimer.twitter
    new TimeoutFilter[Req, Rep](duration, timer)
  }

  val retryCondition: PartialFunction[Try[Nothing], Boolean] = {
    case Throw(error) => error match {
      case e: CancelledConnectionException => true
      case e: FailedFastException => true
      case e: com.twitter.util.TimeoutException => true
      case e: com.twitter.finagle.IndividualRequestTimeoutException => true
      case e =>
        val messageToParse = e.getMessage
        Logger.get().log(Level.ERROR,messageToParse)
        if (messageToParse.contains("Token isn't valid")) {
          token = Await.result(authenticate(login,password))
          true
        } else false
    }
    case _ =>  println("No exception here"); false
  }
  val retryPolicy = RetryPolicy.backoff(Backoff.equalJittered(300.milliseconds, 10.seconds))(retryCondition)
  def retryFilter[Req, Rep] =new RetryExceptionsFilter[Req, Rep](retryPolicy, HighResTimer.Default)

  private val client = Thrift.client.withMonitor(new Monitor {
    def handle(error: Throwable): Boolean = error match {
      case e: com.twitter.util.TimeoutException => true
      case e: Failure => {
        Logger.get().log(Level.ERROR, e.getMessage, e)
        true
      }
      case _ => false
    }
  })

  private val interface= client.newServiceIface[TransactionService.ServiceIface](serverIPAddress, "transaction")
  private val timeoutTime = 150.milliseconds
  private val interfaceCopy = interface.copy(
    putStream =             retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.putStream),
    isStreamExist =         retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.isStreamExist),
    getStream =             retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.getStream),
    delStream =             retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.delStream),
    putTransaction =        retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.putTransaction),
    putTransactions =       retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.putTransactions),
    scanTransactions =      retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.scanTransactions),
    scanTransactionsCRC32 = retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.scanTransactionsCRC32),
    putTransactionData =    retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.putTransactionData),
    getTransactionData =    retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.getTransactionData),
    setConsumerState =      retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.setConsumerState),
    getConsumerState =      retryFilter.andThen(timeoutFilter(timeoutTime) andThen interface.getConsumerState)
  )
  private val request = Thrift.client.newMethodIface(interfaceCopy)

  //Stream API
  override def putStream(token: String, stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = {
    request.putStream(token, stream, partitions, description)
  }
  override def isStreamExist(token: String, stream: String): TwitterFuture[Boolean] = request.isStreamExist(token, stream)
  override def getStream(token: String, stream: String): TwitterFuture[Stream]  = request.getStream(token, stream)
  override def delStream(token: String, stream: String): TwitterFuture[Boolean] = request.delStream(token, stream)

  //TransactionMeta API
  override def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = {
    Await.ready(request.putTransaction(token, transaction))
  }
  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = {
    Await.ready(request.putTransactions(token, transactions))
  }
  override def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = request.scanTransactions(token, stream, partition)
  override def scanTransactionsCRC32(token: String, stream: String, partition: Int): TwitterFuture[Int] = request.scanTransactionsCRC32(token, stream, partition)

  //TransactionData API
  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): TwitterFuture[Boolean] =
    request.putTransactionData(token, stream, partition, transaction, from, data)
  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
    request.getTransactionData(token,stream,partition,transaction,from,to)

  //Consumer API
  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
    request.setConsumerState(token,name,stream,partition,transaction)
  override def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
    request.getConsumerState(token,name,stream,partition)
}

object Client extends App {
 // implicit lazy val context = transactionService.Context(2)
  val client = new Client("ognelis","228",":8080",":8081")

  println(Await.ready(client.putStream(client.token,"1",5, None)))

  val acc: ArrayBuffer[TwitterFuture[Boolean]] = new ArrayBuffer[TwitterFuture[Boolean]]()

    val producerTransactions = (0 to 10).map(_ => new ProducerTransaction {
      override val transactionID: Long = scala.util.Random.nextLong()

      override val state: TransactionStates = TransactionStates.Opened

      override val stream: String = "1"

      override val timestamp: Long = Time.epoch.inNanoseconds

      override val quantity: Int = -1

      override val partition: Int = 0

      override def tll: Long = Time.epoch.inNanoseconds
    })

    val transactions = producerTransactions.map(txn => new Transaction {
      override def producerTransaction: Option[ProducerTransaction] = Some(txn)

      override def consumerTransaction: Option[ConsumerTransaction] = None
    })

  println(Await.ready(client.putTransactions(client.token, transactions)))
}
