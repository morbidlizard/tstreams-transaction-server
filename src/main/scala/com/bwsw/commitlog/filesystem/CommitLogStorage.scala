package com.bwsw.commitlog.filesystem

trait CommitLogStorage extends Ordered[CommitLogStorage]{
  def compare(that: CommitLogStorage): Int = java.lang.Long.compare(getID, that.getID)

  /** Returns an iterator over records */
  def getIterator: CommitLogIterator

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): Array[Byte]

  def getID: Long //= file.getName.dropRight(FilePathManager.DATAEXTENSION.length).toLong

  def getContent: Array[Byte]

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5: Array[Byte]

  /** Checks md5 sum of file with existing md5 sum. Throws an exception when no MD5 exists. */
  def checkMD5(): Boolean = getMD5 sameElements calculateMD5()

  /** Returns true if md5-file exists. */
  def md5Exists(): Boolean
}
