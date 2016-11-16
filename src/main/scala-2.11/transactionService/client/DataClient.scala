package transactionService.client

import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Monitor, Time}
import transactionService.rpc.{ProducerTransaction, TransactionDataService, TransactionStates}


object DataClient extends App {
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

  val ifaceData= client.newServiceIface[TransactionDataService.ServiceIface]("localhost:8080", "data")
  val dataCopy = ifaceData.copy(
    putTransactionData = ifaceData.putTransactionData,
    getTransactionData = ifaceData.getTransactionData
  )
  val requestData = Thrift.client.newMethodIface(dataCopy)

  val txn =  new ProducerTransaction {
    override val transactionID: Long = scala.util.Random.nextLong()

    override val state: TransactionStates = TransactionStates.Opened

    override val stream: String = "1"

    override val timestamp: Long = Time.epoch.inNanoseconds

    override val quantity: Int = -1

    override val partition: Int = 0

    override def tll: Long = Time.epoch.inNanoseconds
  }

  val genData = (0 to 7).map(_=>
    scala.util.Random.nextString(4)
  )

  genData foreach println

  val putResult = requestData.putTransactionData(" ",txn.stream,txn.partition,txn.transactionID,0, genData map (x => java.nio.ByteBuffer.wrap(x.getBytes())))
  println(Await.ready(putResult))


  val getResult = requestData.getTransactionData(" ",txn.stream,txn.partition,txn.transactionID,0,7)
  val res = Await.result(getResult)
  res foreach {x =>
    val sizeOfSlicedData  = x.limit() - x.position()
    val bytes = new Array[Byte](sizeOfSlicedData)
    x.get(bytes)
    println(new String(bytes))
  }
}
