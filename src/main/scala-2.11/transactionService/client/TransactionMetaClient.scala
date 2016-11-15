package transactionService.client

import com.twitter.finagle._
import com.twitter.logging.{Level, Logger}
import com.twitter.util._
import org.apache.thrift.protocol.TCompactProtocol
import transactionService.rpc.TransactionMetaService.ServiceIface
import transactionService.rpc._

object TransactionMetaClient extends App {
  val client = Thrift.client.withMonitor(new Monitor {
    def handle(error: Throwable): Boolean = error match {
      case e: com.twitter.util.TimeoutException => true
      case e: Failure => {
        Logger.get().log(Level.ERROR, e.getMessage, e)
        true
      }
      case _ => false
    }
  })



  val ifaceStream= client.newServiceIface[StreamService.ServiceIface]("localhost:8080", "stream")
  val streamCopy = ifaceStream.copy(
    putStream = ifaceStream.putStream,
    getStream = ifaceStream.getStream
  )
  val requestStream = Thrift.client.withProtocolFactory(new TCompactProtocol.Factory()).newMethodIface(streamCopy)

  val streams = (0 to 0).map(_=> new transactionService.rpc.Stream {
    override val partitions = scala.util.Random.nextInt()
    override val description: Option[String] = Some("asdasdasdsd")
  })

  val names = (0 to 0).map(_=> scala.util.Random.nextInt(100).toString)

  val putResult = (streams zip names) map {case (stream,name) => requestStream.putStream(" ", "stream1", stream.partitions,stream.description)}
  println(Await.ready(Future.collect(putResult)))


  val producerTransactions = (0 to 7).map(_=> new ProducerTransaction {
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

  val ifaceTransaction: ServiceIface = client.newServiceIface[TransactionMetaService.ServiceIface]("localhost:8080", "transaction")
  val transactionCopy = ifaceTransaction.copy(
    putTransaction = ifaceTransaction.putTransaction,
    putTransactions = ifaceTransaction.putTransactions
  )
  val requestTransaction = Thrift.client.newMethodIface(transactionCopy)

  producerTransactions foreach (x => println (x.transactionID))


  val resultsPut = requestTransaction.putTransactions("", transactions)
  println(Await.ready(resultsPut))


  val txn = transactions(scala.util.Random.nextInt(transactions.length))

 // println(Await.ready(requestTransaction.delTransaction("",txn.producerTransaction.get.stream,txn.producerTransaction.get.partition,txn.producerTransaction.get.transactionID)))

//
//  val resultsDelete = transactions map (transaction => request.delTransaction("",transaction.stream,transaction.partition,transaction.transactionID))
//  Await.ready(Future.collectToTry(resultsDelete)) foreach(x=> x.foreach(println(_)))

}
