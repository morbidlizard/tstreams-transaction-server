package transactionService.server.transactionDataService

case class KeyDataSeq(key: Key, dataSeq: Int) {
  override def toString: String = s"${key.toString} $dataSeq"
}
