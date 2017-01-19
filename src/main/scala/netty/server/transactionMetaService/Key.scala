package netty.server.transactionMetaService

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
    Key(long2Long(stream), int2Integer(partition), long2Long(transactionID))
  }

  override def objectToEntry(key: Key, output: TupleOutput): Unit = {
    output.writeLong(Long2long(key.stream))
    output.writeInt(Integer2int(key.partition))
    output.writeLong(Long2long(key.transactionID))
  }
}




