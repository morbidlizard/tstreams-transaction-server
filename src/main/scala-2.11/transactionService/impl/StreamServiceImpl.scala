package transactionService.impl

import com.twitter.util.Future
import transactionService.rpc.{Stream, StreamService}

trait StreamServiceImpl extends StreamService[Future] {

  def putStream(token: String, stream: String, partitions: Int, description: String): Future[Boolean] = ???

  def getStream(token: String, stream: String): Future[Stream] = ???

  def delStream(token: String, stream: String): Future[Boolean] = ???
}
