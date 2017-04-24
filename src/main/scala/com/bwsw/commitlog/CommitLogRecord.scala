package com.bwsw.commitlog

import java.nio.ByteBuffer
import java.util.Base64

import CommitLogRecord._

final class CommitLogRecord(val id: Long, val messageType: Byte, val message: Array[Byte])
{
  def toByteArrayWithDelimiter: Array[Byte] = delimiter +: toByteArrayWithoutDelimiter

  @inline
  def toByteArrayWithoutDelimiter: Array[Byte] = {
    val buffer = ByteBuffer
      .allocate(java.lang.Long.BYTES + java.lang.Byte.BYTES + message.length)
      .putLong(id)
      .put(messageType)
      .put(message)
    buffer.flip()

    base64Encoder.encode(buffer).array()
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case commitLogRecord: CommitLogRecord =>
      id == commitLogRecord.id &&
        messageType == commitLogRecord.messageType &&
        message.sameElements(commitLogRecord.message)
    case _ => false
  }

}
object CommitLogRecord {
  private val delimiter: Byte = 0
  private val base64Encoder: Base64.Encoder = Base64.getEncoder
  private val base64Decoder: Base64.Decoder = Base64.getDecoder

  final def apply(id: Long, messageType: Byte, message: Array[Byte]): CommitLogRecord = new CommitLogRecord(id, messageType, message)

  final def fromByteArrayWithoutDelimiter(bytes: Array[Byte]): Either[IllegalArgumentException, CommitLogRecord] = {
    scala.util.Try {
      val decodedRecord = base64Decoder.decode(bytes)
      val (idBinary, messageTypeWithMessage) = decodedRecord.splitAt(java.lang.Long.BYTES)
      val id = java.nio.ByteBuffer.wrap(idBinary).getLong
      val messageType = messageTypeWithMessage.head
      val message = messageTypeWithMessage.tail
      new CommitLogRecord(id, messageType, message)
    } match {
      case scala.util.Success(binaryRecord) => scala.util.Right(binaryRecord)
      case scala.util.Failure(_) => scala.util.Left(new IllegalArgumentException("Commit log record is corrupted"))
    }
  }

  final def fromByteArrayWithDelimiter(bytes: Array[Byte]): Either[IllegalArgumentException, CommitLogRecord] = fromByteArrayWithoutDelimiter(bytes.tail)
}
