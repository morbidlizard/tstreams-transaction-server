package netty.server.transactionDataService

import `implicit`.Implicits._

case class Key(partition: Int, transaction: Long) {
  def toBinary: Array[Byte] = intToByteArray(partition) ++ longToByteArray(transaction)
  override def toString: String = s"$partition $transaction"
}

object Key {
  val size = 8
}