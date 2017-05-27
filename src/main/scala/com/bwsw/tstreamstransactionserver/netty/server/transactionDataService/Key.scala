package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits.intToByteArray


case class Key(partition: Int, transaction: Long) {
  lazy private val binaryKey: Array[Byte] = toByteArray()
  final def toByteArray(): Array[Byte] = {
    val size = java.lang.Integer.BYTES + java.lang.Long.BYTES
    val buffer = java.nio.ByteBuffer
      .allocate(size)
      .putInt(partition)
      .putLong(transaction)
    buffer.flip()

    val binaryArray = new Array[Byte](size)
    buffer.get(binaryArray)
    binaryArray
  }

  final def toByteArray(dataID: Int): Array[Byte] = binaryKey ++: intToByteArray(dataID)
  override def toString: String = s"$partition $transaction"
}

object Key {
  val size = 8
}