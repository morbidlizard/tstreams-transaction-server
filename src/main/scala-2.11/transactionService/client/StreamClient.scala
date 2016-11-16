package transactionService.client

import com.twitter.finagle.{Failure, Thrift}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Await, Future, Monitor}
import transactionService.rpc.StreamService

object StreamClient extends App {
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
  val request = Thrift.client.newMethodIface(streamCopy)

  val streams = (0 to 7).map(_=> new transactionService.rpc.Stream {
    override val partitions = scala.util.Random.nextInt()
    override val description: Option[String] = Some("asdasdasdsd")
  })

  val names = (0 to 7).map(_=> scala.util.Random.nextInt(100).toString)

  val putResult = (streams zip names) map {case (stream,name) => request.putStream(" ", name,stream.partitions,stream.description)}
  println(Await.ready(Future.collect(putResult)))

  val getResult = names map (name=> request.getStream("", name))
  println(Await.ready(Future.collect(getResult)))
}
