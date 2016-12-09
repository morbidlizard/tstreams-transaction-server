package transactionService.server.сonsumerService

import com.sleepycat.persist.model.{KeyField, Persistent}

@Persistent
class ConsumerKey {
  @KeyField(1) var name: String = _
  @KeyField(2) var stream: java.lang.Long = _
  @KeyField(3) var partition: Int = _
  def this(name: String, stream: java.lang.Long, partition:Int) = {
    this()
    this.name = name
    this.stream = stream
    this.partition = partition
  }
  override def toString: String = s"consumer:$name\tstream:$stream\tpartition:$partition\t"
}
