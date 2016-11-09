package transactionService.impl

import com.twitter.util.Future
import transactionService.rpc.ConsumerService

trait ConsumerServiceImpl extends ConsumerService[Future] {
  def getConsumerState(token: String, name: String, stream: String, partition: Int): Future[Long] = ???

  def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = ???
}
