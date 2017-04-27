package com.bwsw.tstreamstransactionserver.netty.server.streamService

import java.nio.charset.StandardCharsets

import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamValue.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class StreamValue(name: String, partitions: Int, description: Option[String], ttl: Long, timestamp: Long, @volatile var deleted: Boolean)
  extends com.bwsw.tstreamstransactionserver.rpc.Stream
{
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  def toByteArray: Array[Byte] = {
    val nameBodyFiledSize = java.lang.Integer.BYTES
    val partitionsFieldSize = java.lang.Integer.BYTES
    val descriptionFlagFieldSize = java.lang.Byte.BYTES
    val descriptionFieldSize = java.lang.Integer.BYTES
    val ttlFieldSize = java.lang.Long.BYTES
    val timestampFieldSize = java.lang.Long.BYTES
    val deletedFieldSize = java.lang.Byte.BYTES

    val nameBodyBytes = name.getBytes(StreamValue.charset)
    val descriptionOptionFlag: Byte = description.map(_ => 1:Byte).getOrElse(0:Byte)
    val descriptionBodyBytes = description.map(_.getBytes(StreamValue.charset)).getOrElse(Array[Byte]())

    val buffer = java.nio.ByteBuffer.allocate(
      nameBodyFiledSize + nameBodyBytes.length + partitionsFieldSize +
        descriptionFlagFieldSize + descriptionFieldSize +
        ttlFieldSize + timestampFieldSize + descriptionBodyBytes.length +
        deletedFieldSize
    )

    buffer
      .putInt(name.length)
      .put(nameBodyBytes)
      .putInt(partitions)
      .put(descriptionOptionFlag)
      .putInt(descriptionBodyBytes.length)
      .put(descriptionBodyBytes)
      .putLong(ttl)
      .putLong(timestamp)
      .put(if (deleted) 1:Byte else 0:Byte)
      .array()
  }
}

object StreamValue extends TupleBinding[StreamValue]
{
  private val charset = StandardCharsets.UTF_8

  override def entryToObject(input: TupleInput): StreamValue = {
    val partitions       = input.readInt()
    val name             = input.readString()
    val description      = input.readString() match {
      case null => None
      case str => Some(str)
    }
    val ttl              = input.readLong()
    val timestamp        = input.readLong()
    val deleted          = input.readBoolean()
    StreamValue(name, partitions, description, ttl, timestamp, deleted)
  }
  override def objectToEntry(stream: StreamValue, output: TupleOutput): Unit = {
    output.writeInt(stream.partitions)
    output.writeString(stream.name)
    stream.description match {
      case Some(description) => output.writeString(description)
      case None => output.writeString(null: String)
    }
    output.writeLong(stream.ttl)
    output.writeLong(stream.timestamp)
    output.writeBoolean(stream.deleted)
  }

  def fromByteArray(bytes: Array[Byte]): StreamValue = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val nameLength = buffer.getInt
    val name = {
      val bytes = new Array[Byte](nameLength)
      buffer.get(bytes)
      new String(bytes, charset)
    }
    val partitions = buffer.getInt
    val isDescriptionOptional = {
      val flag = buffer.get()
      if (flag == (1:Byte)) true else false
    }
    val descriptionLength = buffer.getInt
    val descriptionBody = {
      val bytes = new Array[Byte](descriptionLength)
      buffer.get(bytes)
      new String(bytes, charset)
    }
    val description =
      if (isDescriptionOptional)
        Some(descriptionBody)
      else
        None
    val ttl = buffer.getLong
    val timestamp = buffer.getLong
    val deleted = {
      val flag = buffer.get()
      if (flag == (1:Byte)) true else false
    }
    StreamValue(name, partitions, description, ttl, timestamp, deleted)
  }
}