package transactionService.server.сonsumerService

import com.sleepycat.persist.model.{Entity, PrimaryKey}

@Entity
class ConsumerTransaction extends transactionService.rpc.ConsumerTransaction {
  @PrimaryKey private var key: ConsumerKey = _
  private var transactionIDDB: java.lang.Long = _

  def this(name: String,
           stream: String,
           partition: Int,
           transactionID: java.lang.Long
          ) {
    this()
    this.transactionIDDB = transactionID
    this.key = new ConsumerKey(name, stream, partition)
  }

  override def transactionID: Long = transactionIDDB
  override def name: String = key.name
  override def stream: String = key.stream
  override def partition: Int = key.partition
  override def toString: String = s"Consumer transaction: ${key.toString}"
}
