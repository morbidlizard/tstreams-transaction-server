package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamWithoutKey.objectToEntry
import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry

case class StreamWithoutKey(name: String, partitions: Int, description: Option[String], ttl: Long, timestamp: Long, @volatile var deleted: Boolean)
  extends com.bwsw.tstreamstransactionserver.rpc.Stream
{
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object StreamWithoutKey extends TupleBinding[StreamWithoutKey]
{
  override def entryToObject(input: TupleInput): StreamWithoutKey = {
    val partitions       = input.readInt()
    val name             = input.readString()
    val description      = input.readString() match {
      case null => None
      case str => Some(str)
    }
    val ttl              = input.readLong()
    val timestamp        = input.readLong()
    val deleted          = input.readBoolean()
    StreamWithoutKey(name, partitions, description, ttl, timestamp, deleted)
  }
  override def objectToEntry(stream: StreamWithoutKey, output: TupleOutput): Unit = {
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