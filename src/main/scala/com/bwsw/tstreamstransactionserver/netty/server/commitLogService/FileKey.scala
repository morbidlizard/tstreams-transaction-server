package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

class FileKey(val id: Long) extends AnyVal {
  final def toByteArray = java.nio.ByteBuffer.allocate(java.lang.Long.BYTES).putLong(id).array()
}

object FileKey {
  def apply(id: Long): FileKey = new FileKey(id)
  final def fromByteArray(bytes: Array[Byte]) = new FileKey(java.nio.ByteBuffer.wrap(bytes).getLong)
}

