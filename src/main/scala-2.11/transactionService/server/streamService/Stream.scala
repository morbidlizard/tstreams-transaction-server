package transactionService.server.streamService

import com.sleepycat.persist.model.{Entity, PrimaryKey}

@Entity class Stream extends transactionService.rpc.Stream {
  @PrimaryKey private var nameDB: String = _
  private var partitionsDB: Int = _
  private var descriptionDB: String = _
  private var ttlDB: Int   = _

  override def partitions: Int = partitionsDB
  override def description: Option[String] = Option(descriptionDB)
  override def name: String = nameDB
  override def ttl: Int = ttlDB

  def this(name: String, partitions:Int, description: Option[String], ttl: Int) = {
    this()
    nameDB = name
    partitionsDB = partitions
    ttlDB = ttl
    description foreach (str => descriptionDB = str)
  }
}
