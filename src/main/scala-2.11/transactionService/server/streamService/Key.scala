package transactionService.server.streamService

import com.sleepycat.bind.tuple.{TupleBinding, TupleInput, TupleOutput}
import Key.objectToEntry
import com.sleepycat.je.DatabaseEntry

case class Key(streamNameToLong: java.lang.Long) extends AnyVal {
  def toDatabaseEntry: DatabaseEntry = {
    val databaseEntry = new DatabaseEntry()
    objectToEntry(this, databaseEntry)
    databaseEntry
  }
}

object Key extends TupleBinding[Key] {
  override def entryToObject(input: TupleInput): Key = Key(input.readLong())
  override def objectToEntry(key: Key, output: TupleOutput): Unit = output.writeLong(key.streamNameToLong)
}
