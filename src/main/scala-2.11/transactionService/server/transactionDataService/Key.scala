package transactionService.server.transactionDataService

import `implicit`.Implicits._
import Key._

case class Key(stream: java.lang.Long, partition: Int, transaction: Long) {
  def toBinary: Array[Byte] =
    longToByteArray(stream) ++ delimiter ++
      longToByteArray(transaction) ++ delimiter ++
      intToByteArray(partition)

  override def toString: String = s"$stream $partition $transaction"
}

private object Key {
  val delimiter: Array[Byte] = Array.fill(1)(Byte.MinValue)
}
