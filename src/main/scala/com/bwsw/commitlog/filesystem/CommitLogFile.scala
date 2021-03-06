package com.bwsw.commitlog.filesystem

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

import org.apache.commons.io.IOUtils

/** Represents commitlog file with data.
  *
  * @param path full path to file
  */
class CommitLogFile(path: String) extends CommitLogStorage{
  //todo CommitLogFile существует в двух реализациях: private класс(внутри CommitLog) и этот класс. Требуется рефакторинг (может достаточно переименования)
  private val file = {
    val file = new File(path)
    if (file.exists()) file else throw new IOException(s"File ${file.getPath} doesn't exist!")
  }

  private val md5File = new File(file.toString.split('.')(0) + FilePathManager.MD5EXTENSION)

  class Attributes {
    private val attr = Files.readAttributes(Paths.get(file.getAbsolutePath), classOf[BasicFileAttributes])
    def creationTime = attr.creationTime()
    def lastAccessTime = attr.lastAccessTime()
    def lastModifiedTime = attr.lastModifiedTime()
  }

  def attributes = new Attributes()

  /** Returns underlying file. */
  def getFile: File = file


  /** Returns an iterator over records */
  override def getIterator: CommitLogIterator = new CommitLogFileIterator(file.toString)


  /** bytes to read from this file */
  private val chunkSize = 100000

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): Array[Byte] = {
    val fileInputStream = new FileInputStream(file)
    val stream = new BufferedInputStream(fileInputStream)

    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.reset()
    while (stream.available() > 0) {
      val chunk = new Array[Byte](chunkSize)
      val bytesRead = stream.read(chunk)
      md5.update(chunk.take(bytesRead))
    }

    stream.close()
    fileInputStream.close()

    DatatypeConverter.printHexBinary(md5.digest()).getBytes
  }

  /** Returns a MD5 sum from MD5 FIle */
  private def getContentOfMD5File = {
    val fileInputStream = new FileInputStream(md5File)
    val md5Sum = new Array[Byte](32)
    fileInputStream.read(md5Sum)
    fileInputStream.close()
    md5Sum
  }

  override final def getContent: Array[Byte] = {
    val fileInputStream = new FileInputStream(file)
    val content = IOUtils.toByteArray(fileInputStream)
    fileInputStream.close()
    content
  }

  final def getID: Long = file.getName.dropRight(FilePathManager.DATAEXTENSION.length).toLong


  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5: Array[Byte] = if (!md5Exists()) throw new FileNotFoundException("No MD5 file for " + path) else getContentOfMD5File

  /** Returns true if md5-file exists. */
  def md5Exists(): Boolean = md5File.exists()

  /** Delete file */
  def delete(): Boolean = {
    file.delete() && (if (md5Exists()) md5File.delete() else true)

  }
}
