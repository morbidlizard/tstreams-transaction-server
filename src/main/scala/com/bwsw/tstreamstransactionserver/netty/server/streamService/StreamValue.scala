package com.bwsw.tstreamstransactionserver.netty.server.streamService

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
}

object StreamValue extends TupleBinding[StreamValue]
{
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
}