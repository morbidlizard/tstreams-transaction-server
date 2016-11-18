package transactionService.server.transactionDataService

case class Key(stream: String, partition: Int, transaction: Long) {
  override def toString: String = s"$stream $partition $transaction"
}
