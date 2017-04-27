package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import java.nio.charset.StandardCharsets

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class ConsumerTransactionKey(name: String, stream: java.lang.Long, partition: java.lang.Integer) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }

  def toByteArray: Array[Byte] = {
    val nameBinary = name.getBytes(ConsumerTransactionKey.charset)
    val nameFieldSize = java.lang.Integer.BYTES
    val buffer = java.nio.ByteBuffer.allocate(
      nameFieldSize + nameBinary.length + java.lang.Long.BYTES + java.lang.Integer.BYTES)

    buffer
      .putInt(nameBinary.length)
      .put(nameBinary)
      .putLong(stream)
      .putInt(partition)
      .array()
  }

}

object ConsumerTransactionKey extends TupleBinding[ConsumerTransactionKey] {
  val charset = StandardCharsets.UTF_8

  override def entryToObject(input: TupleInput): ConsumerTransactionKey = {
    val name = input.readString()
    val stream = input.readLong()
    val partition = input.readInt()
    ConsumerTransactionKey(name, stream, partition)
  }

  override def objectToEntry(key: ConsumerTransactionKey, output: TupleOutput): Unit = {
    output.writeString(key.name)
    output.writeLong(key.stream)
    output.writeInt(key.partition)
  }

  def fromByteArray(bytes: Array[Byte]): ConsumerTransactionKey = {
    val buffer     = java.nio.ByteBuffer.wrap(bytes)
    val nameLength = buffer.getInt
    val name = {
      val bytes = new Array[Byte](nameLength)
      buffer.get(bytes)
      new String(bytes, charset)
    }
    val stream = buffer.getLong
    val partition = buffer.getInt
    ConsumerTransactionKey(name, stream, partition)
  }
}
