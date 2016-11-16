package transactionService.client

import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Monitor}
import transactionService.rpc.ConsumerService

object ConsumerClient extends App {
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

  val ifaceConsumer= client.newServiceIface[ConsumerService.ServiceIface]("localhost:8080", "consumer")
  val consumerCopy = ifaceConsumer.copy(
    setConsumerState = ifaceConsumer.setConsumerState,
    getConsumerState = ifaceConsumer.getConsumerState
  )
  val requestConsumer = Thrift.client.newMethodIface(consumerCopy)

  val putResult = requestConsumer.setConsumerState("","consumer1","1",0,22224444L)
  println(Await.ready(putResult))

  val getResult = requestConsumer.getConsumerState("","consumer1","1",0)
  println(Await.ready(getResult))
}