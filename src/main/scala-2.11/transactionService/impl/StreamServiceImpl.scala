package transactionService.impl

import com.sleepycat.persist.model.Persistent
import com.twitter.util.Future
import transactionService.rpc.{Stream, StreamService}

trait StreamServiceImpl extends StreamService[Future] {

  def putStream(token: String, stream: String, partitions: Int, description: String): Future[Boolean] = ???

  def getStream(token: String, stream: String): Future[Stream] = ???

  def delStream(token: String, stream: String): Future[Boolean] = ???
}

object StreamServiceImpl {

  @Persistent class StreamSleepyCat() extends Stream {
    private var partitionsMutuable: Int = _
    private var descriptionMutuable: Option[String] = _

    override val partitions = this.partitionsMutuable
    override val description = this.descriptionMutuable


    def this(partitions: Int,description: Option[String]) = {
      this()
      this.partitionsMutuable = partitions
      this.descriptionMutuable = description
    }
  }

}
