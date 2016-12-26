package netty.server.transactionDataService

import `implicit`.Implicits._

case class Key(transaction: Long) extends AnyVal {
  def toBinary: Array[Byte] = longToByteArray(transaction)
  override def toString: String = s"$transaction"
}

object Key {
  val size = 8
}