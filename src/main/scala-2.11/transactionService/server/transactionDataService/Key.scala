package transactionService.server.transactionDataService

import `implicit`.Implicits._

case class Key(stream: java.lang.Long, partition: Int, transaction: Long) {
  def toBinary: Array[Byte] =
    longToByteArray(stream) ++
      longToByteArray(transaction) ++
      intToByteArray(partition)

  override def toString: String = s"$stream $partition $transaction"
}