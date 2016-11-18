package transactionService.server.streamService

import com.sleepycat.persist.model.{Entity, PrimaryKey}

@Entity class Stream extends transactionService.rpc.Stream {
  @PrimaryKey private var nameDB: String = _
  private var partitionsDB: Int = _
  private var descriptionDB:  String = _

  override def partitions: Int = partitionsDB
  override def description: Option[String] = Option(descriptionDB)

  def this(name: String, partitions:Int, description: Option[String]) = {
    this()
    nameDB = name
    partitionsDB = partitions
    description foreach (str => descriptionDB = str)
  }
}
