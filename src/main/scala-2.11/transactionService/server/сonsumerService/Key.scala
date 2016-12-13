package transactionService.server.сonsumerService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import Key.objectToEntry

case class Key(name: String, stream: java.lang.Long, partition: java.lang.Integer) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  override def toString: String = s"name:$name\tstream:$stream\tpartition:$partition"
}

object Key extends TupleBinding[Key] {
  override def entryToObject(input: TupleInput): Key = {
    val name = input.readString()
    val stream = input.readLong()
    val partition = input.readInt()
    Key(name, stream, partition)
  }

  override def objectToEntry(key: Key, output: TupleOutput): Unit = {
    output.writeChars(key.name)
    output.writeLong(key.stream)
    output.writeInt(key.partition)
  }
}
