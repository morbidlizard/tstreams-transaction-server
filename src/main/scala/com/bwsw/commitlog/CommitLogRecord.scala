package com.bwsw.commitlog

import java.nio.ByteBuffer

import CommitLogRecord._

/** look at [[com.bwsw.commitlog.CommitLogRecordHeader]] when change attributes of this class  */
final class CommitLogRecord(val id: Long, val messageType: Byte, val message: Array[Byte], val timestamp: Long = System.currentTimeMillis())
{
  @inline
  def toByteArray: Array[Byte] = {
    ByteBuffer.allocate(size)
      .putLong(id)
      .put(messageType)
      .putInt(message.length)
      .putLong(timestamp)
      .put(message)
      .array()
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case commitLogRecord: CommitLogRecord =>
      id == commitLogRecord.id &&
        messageType == commitLogRecord.messageType &&
        message.sameElements(commitLogRecord.message)
    case _ => false
  }

  def size: Int = headerSize + message.length
}
object CommitLogRecord {
  val headerSize: Int =
    java.lang.Long.BYTES +     //id
    java.lang.Byte.BYTES +     //messageType
    java.lang.Integer.BYTES +  //messageLength
    java.lang.Long.BYTES       //timestamp


  final def apply(id: Long, messageType: Byte, message: Array[Byte], timestamp: Long): CommitLogRecord = new CommitLogRecord(id, messageType, message, timestamp)

  final def fromByteArray(bytes: Array[Byte]): Either[IllegalArgumentException, CommitLogRecord] = {
    scala.util.Try {
      val buffer = ByteBuffer.wrap(bytes)
      val id     = buffer.getLong()
      val messageType   = buffer.get()
      val messageLength = buffer.getInt()
      val timestamp     = buffer.getLong()
      val message       = {
        val binaryMessage = new Array[Byte](messageLength)
        buffer.get(binaryMessage)
        binaryMessage
      }
      new CommitLogRecord(id, messageType, message, timestamp)
    } match {
      case scala.util.Success(binaryRecord) => scala.util.Right(binaryRecord)
      case scala.util.Failure(_) => scala.util.Left(new IllegalArgumentException("Commit log record is corrupted"))
    }
  }
}
