package netty.server.transactionDataService

case class StorageName(stream: String) extends AnyVal{
  override def toString: String = stream
}
