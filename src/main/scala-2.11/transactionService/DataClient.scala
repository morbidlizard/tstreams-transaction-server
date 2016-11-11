package transactionService

import com.twitter.finagle.{Failure, Thrift}
import com.twitter.io.Buf.ByteBuffer
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Future, Monitor, Time}
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TCompactProtocol.Factory
import org.apache.thrift.transport.TIOStreamTransport
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

  val ifaceData= client.newServiceIface[TransactionDataService.ServiceIface]("localhost:8082", "data")
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
    java.nio.ByteBuffer.allocate(8)
      .putInt(scala.util.Random.nextInt(200))
      .putInt(scala.util.Random.nextInt(200))
  )

  val putResult = requestData.putTransactionData(" ",txn.stream,txn.partition,txn.transactionID,0,genData)
  println(Await.ready(putResult))


  val getResult = requestData.getTransactionData(" ",txn.stream,txn.partition,txn.transactionID,2,6)
  val res = println(Await.result(getResult).map(_.array().length))
}
