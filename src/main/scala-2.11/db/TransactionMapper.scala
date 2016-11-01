package db

import java.nio.ByteBuffer

import org.rocksdb._
import transactionServer.rpc.Transaction
import TransactionMapper._

class TransactionMapper(path: String) extends MyRocksDb(path) {

  implicit def strToByteString(str: String): Array[Byte]  = str.getBytes
  implicit def intToByteString(int: Int): Array[Byte]     = ByteBuffer.allocate(4).putInt(int).array()
  implicit def floatToByteString(long: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(long).array()


  def putTransaction : Transaction => Boolean = { transaction =>
    val stream = transaction._1
    val partition = transaction._2
    val interval  = transaction._3
    val transactionId = transaction._4
    val state = transaction._5
    val quantity = transaction._6
    val timestamp = transaction._7

    val (client, map) = connection

    val key = s"$stream $partition $transactionId"

    val handlerOpt = familyHandlerByDescriptorName(key, map)
    val familyHandler = if (handlerOpt.isEmpty) {
      val descriptor = new ColumnFamilyDescriptor(key)
      val handler    = client.createColumnFamily(descriptor)
      handler
    } else {
      handlerOpt.get
    }

    val batch = new WriteBatch()
    batch.put(familyHandler, stateName, state.value)
    batch.put(familyHandler, quantityName, quantity)
    batch.put(familyHandler, timestampName, timestamp)

    client.write(new WriteOptions(), batch)

    batch.close()
    familyHandler.close()
    client.close()
    true
  }
}

object TransactionMapper{
  val streamName = "stream"
  val partitionName  = "partition"
  val interval = "interval"
  val transactionIdName = "txn"
  val stateName = "state"
  val quantityName = "qty"
  val timestampName = "timestamp"
}
