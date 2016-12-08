package transactionService.server.transactionDataService

import transactionService.server.`implicit`.Implicits._
import Key._

case class Key(stream: String, partition: Int, transaction: Long) {
  def toBinary: Array[Byte] =
    strToByteArray(stream) ++ strToByteArray(delimiter) ++
      longToByteArray(transaction) ++ strToByteArray(delimiter) ++
      intToByteArray(partition)

  override def toString: String = s"$stream $partition $transaction"
}

private object Key {
  val delimiter = " "
}
