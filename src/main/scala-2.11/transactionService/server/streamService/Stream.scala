package transactionService.server.streamService

import com.sleepycat.bind.tuple.TupleBinding
import com.sleepycat.bind.tuple.TupleInput
import com.sleepycat.bind.tuple.TupleOutput
import com.sleepycat.je.DatabaseEntry
import Stream.objectToEntry

case class Stream(name: String, partitions: Int, description: Option[String], ttl: Int)
  extends transactionService.rpc.Stream
{
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object Stream extends TupleBinding[Stream]
{
  override def entryToObject(input: TupleInput): Stream = {
    val partitions       = input.readInt()
    val ttl              = input.readInt()
    val name             = input.readString()
    val description      = input.readString() match {
      case null => None
      case str => Some(str)
    }
    Stream(name, partitions, description, ttl)
  }
  override def objectToEntry(stream: Stream, output: TupleOutput): Unit = {
    output.writeInt(stream.partitions)
    output.writeInt(stream.ttl)
    output.writeString(stream.name)
    stream.description match {
      case Some(description) => output.writeString(description)
      case None => output.writeString(null: String)
    }
  }
}