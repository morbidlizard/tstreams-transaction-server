package transactionService.server.transactionDataService

case class KeyDataSeq(key: Key, dataSeq: Int) {
  private def dataSeqToBinary = String.format("%16s", Integer.toBinaryString(dataSeq)).replace(' ', '0')
  override def toString: String = s"$dataSeqToBinary ${key.toString}"
}
