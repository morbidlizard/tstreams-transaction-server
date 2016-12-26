package netty.server.transactionDataService

case class StorageName(stream: String, partition: Int) {
  override def toString: String = s"${stream}_$partition"
}
