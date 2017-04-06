package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

class FileValue(val fileContent: Array[Byte], val fileMD5Content: Option[Array[Byte]]) {
  final def toByteArray = fileMD5Content match {
    case Some(md5Content) =>
      require(md5Content.length == FileValue.MD5_SUM_LENGTH)
      (FileValue.MD5_FILE_EXIST +: md5Content) ++ fileContent
    case None =>
      FileValue.NO_MD5_FILE +: fileContent
  }

  override def hashCode(): Int = {
    val prime = 31
    val md5HashCode =  if (fileMD5Content.isDefined)
      java.util.Arrays.hashCode(fileMD5Content.get)
    else
      1
    prime*java.util.Arrays.hashCode(fileContent) + md5HashCode
  }

  override def equals(that: scala.Any): Boolean = that match {
    case fileValue: FileValue =>
      (fileContent sameElements fileValue.fileContent) && ((fileMD5Content, fileValue.fileMD5Content) match {
        case (Some(thisMD5), Some(thatMD5)) => thisMD5 sameElements thatMD5
        case (None, None) => true
        case _ => false
      })
    case _ => false
  }
}

object FileValue {
  private val NO_MD5_FILE = 1: Byte
  private val MD5_FILE_EXIST = 2: Byte
  private val MD5_SUM_LENGTH = 32

  def apply(fileContent: Array[Byte], fileMD5Content: Option[Array[Byte]]): FileValue = new FileValue(fileContent, fileMD5Content)

  final def fromByteArray(bytes: Array[Byte]) = {
    val md5_flag = bytes.head
    val other = bytes.tail
    if (md5_flag == NO_MD5_FILE)
      new FileValue(other, None)
    else {
      val (md5Sum, fileContent) = other.splitAt(MD5_SUM_LENGTH)
      new FileValue(fileContent, Some(md5Sum))
    }
  }
}
