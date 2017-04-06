package com.bwsw.commitlog.filesystem

abstract class CommitLogStorage {
  /** Returns an iterator over records */
  def getIterator: CommitLogIterator

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): Array[Byte]

  def getID: Long //= file.getName.dropRight(FilePathManager.DATAEXTENSION.length).toLong

  def getContent: Array[Byte]

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5: Array[Byte] //= if (!md5Exists()) throw new FileNotFoundException("No MD5 file for " + path) else getContentOfMD5File

  /** Checks md5 sum of file with existing md5 sum. Throws an exception when no MD5 exists. */
  def checkMD5(): Boolean = getMD5 sameElements calculateMD5()

  /** Returns true if md5-file exists. */
  def md5Exists(): Boolean //= md5File.exists()
}
