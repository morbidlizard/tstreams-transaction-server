package util.db

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabase, KeyValueDatabaseBatch}

import scala.collection.mutable.ArrayBuffer

class KeyValueDatabaseBatchInMemory(dbs: Array[KeyValueDatabase])
  extends KeyValueDatabaseBatch
{
  private val operationBuffer = new ArrayBuffer[Unit => Unit]()

  override def put(index: Int, key: Array[Byte], data: Array[Byte]): Boolean =
    this.synchronized {
      def operation: Unit => Unit =
        Unit => dbs(index).put(key, data)
      operationBuffer += operation
      true
    }

  override def remove(index: Int, key: Array[Byte]): Unit =
    this.synchronized {
      val operation: Unit => Unit =
        Unit => dbs(index).delete(key)
      operationBuffer += operation
    }

  override def write(): Boolean = {
    operationBuffer.foreach(operation =>
      operation(())
    )
    true
  }
}
