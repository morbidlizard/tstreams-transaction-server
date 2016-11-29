package transactionService.server.transactionMetaService

import com.sleepycat.persist.model.{Entity, PrimaryKey, Relationship, SecondaryKey}
import transactionService.rpc.TransactionStates

@Entity
class ProducerTransaction extends transactionService.rpc.ProducerTransaction {
  @PrimaryKey var key: ProducerTransactionKey = _
  @SecondaryKey(relate = Relationship.MANY_TO_ONE) var stateDB: Int = _
  private var timestampDB: java.lang.Long = _
  private var quantityDB: Int = _

  def this(transactionID: java.lang.Long,
           state: TransactionStates,
           stream: String,
           timestamp: java.lang.Long,
           quantity: Int,
           partition: Int) {
    this()
    this.stateDB = state.getValue()
    this.timestampDB = timestamp
    this.quantityDB = quantity
    this.key = new ProducerTransactionKey(stream, partition, transactionID)
  }

  override def transactionID: Long = key.transactionID
  override def state: TransactionStates = TransactionStates(stateDB)
  override def stream: String = key.stream
  override def timestamp: Long = timestampDB
  override def quantity: Int = quantityDB
  override def partition: Int = key.partition

  override def toString: String = s"Producer transaction: ${key.toString}"
}
