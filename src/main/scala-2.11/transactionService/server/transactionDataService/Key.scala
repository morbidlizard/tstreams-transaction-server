package transactionService.server.transactionDataService

import transactionService.server.`implicit`.Implicits._
import Key._

case class Key(stream: String, partition: Int, transaction: Long) {
  def toBinary: Array[Byte] =
    strToByteArray(stream) ++ strToByteArray(delimeter) ++
    intToByteArray(partition) ++ strToByteArray(delimeter) ++
    longToByteArray(transaction)

  override def toString: String = s"$stream $partition $transaction"
}

private object Key {
  val delimeter = " "
}
