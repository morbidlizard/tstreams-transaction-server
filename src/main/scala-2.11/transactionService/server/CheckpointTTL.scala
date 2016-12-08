package transactionService.server
import com.twitter.util.{Future => TwitterFuture}

trait CheckpointTTL {
  val streamTTL = new java.util.concurrent.ConcurrentHashMap[String, Int]()
  def getStreamTTL(stream: String): Int
}
