package transactionService.server.transactionMetaService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import com.sleepycat.je.DatabaseEntry
import Key.objectToEntry

case class Key(stream: java.lang.Long, partition: java.lang.Integer, transactionID: java.lang.Long) {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
  override def toString: String = s"stream:$stream\tpartition:$partition\tid:$transactionID"
}

object Key extends TupleBinding[Key] {
  override def entryToObject(input: TupleInput): Key = {
    val stream = input.readLong()
    val partition = input.readInt()
    val transactionID = input.readLong()
    Key(stream, partition, transactionID)
  }

  override def objectToEntry(key: Key, output: TupleOutput): Unit = {
    output.writeLong(key.stream)
    output.writeInt(key.partition)
    output.writeLong(key.transactionID)
  }
}




