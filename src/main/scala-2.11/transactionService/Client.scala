package transactionService

import com.twitter.finagle._
import com.twitter.logging.{Level, Logger}
import com.twitter.util._
import transactionService.rpc.{Transaction, TransactionMetaService, TransactionStates}
import transactionService.rpc.TransactionMetaService.ServiceIface

object Client extends App {
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

  val iface: ServiceIface = client.newServiceIface[TransactionMetaService.ServiceIface]("localhost:8080", "transaction")
  val transactionCopy = iface.copy(iface.putTransaction)
  val request = Thrift.client.newMethodIface(transactionCopy)

//  val transaction = new Transaction {
//    override val transactionID: Long = 111111111222222L
//
//    override val state: TransactionStates = TransactionStates.Opened
//
//    override val stream: String = "stream1"
//
//    override val timestamp: Long = Time.epoch.inNanoseconds
//
//    override val interval: Long = Time.epoch.inNanoseconds
//
//    override val quantity: Int = -1
//
//    override val partition: Int = 0
//  }
//  val result = request.putTransaction("",transaction)
//  println(Await.ready(result))

  val transactions = (1 to 5).map(_=> new Transaction {
    override val transactionID: Long = scala.util.Random.nextLong()

    override val state: TransactionStates = TransactionStates.Opened

    override val stream: String = "stream1"

    override val timestamp: Long = Time.epoch.inNanoseconds

    override val interval: Long = Time.epoch.inNanoseconds

    override val quantity: Int = -1

    override val partition: Int = 0
  })

  val results = request.putTransactions("", transactions)
  println(Await.ready(results))



}
