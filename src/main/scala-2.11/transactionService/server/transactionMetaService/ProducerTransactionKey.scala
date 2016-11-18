package transactionService.server.transactionMetaService

import com.sleepycat.persist.model.{KeyField, Persistent}

@Persistent
class ProducerTransactionKey {
  @KeyField(1) var stream: String = _
  @KeyField(2) var partition: Int = _
  @KeyField(3) var transactionID: java.lang.Long = _
  def this(stream: String, partition:Int, transactionID: java.lang.Long) = {
    this()
    this.stream = stream
    this.partition = partition
    this.transactionID = transactionID
  }

  override def toString: String = s"stream:$stream\tpartition:$partition\tid:$transactionID"
}
