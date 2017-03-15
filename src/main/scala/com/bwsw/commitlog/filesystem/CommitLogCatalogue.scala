package com.bwsw.commitlog.filesystem

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils

object CommitLogCatalogue {
  val MD5EXTENSION = ".md5"
  val DATAEXTENSION = ".dat"
}

/** Represents catalogue of specified date.
  *
  * @param rootPath path to the root directory of commitlog
  * @param date date to link this object with
  */
class CommitLogCatalogue(rootPath: String, date: Date) {
  private val rootDirectory: String = rootPath


  private val dataFolder: File = {
    val simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    simpleDateFormat.setLenient(false)
    new File(rootDirectory, simpleDateFormat.format(date))
  }

  /** Removes specified file and its md5 file.
    *
    * @param fileName name of file to delete
    * @return true if file and its md5 file were deleted successfully
    */
  def deleteFile(fileName: String): Boolean = {
    val file = new File(dataFolder.toString, fileName)
    file.delete() &&
      new File(file.toString.split("\\.")(0) + CommitLogCatalogue.MD5EXTENSION).delete()
  }

  /** Deletes all files in directory.
    *
    * @return true if all files were deleted successfully
    */
  def deleteAllFiles(): Boolean = {
    var res: Boolean = true
    for (file <- dataFolder.listFiles()) {
      res &= file.delete()
    }
    res
  }

  /** Returns all files in directory. */
  def listAllFiles(): Seq[CommitLogFile] = {
    dataFolder.listFiles()
      .filter(file => file.toString endsWith CommitLogCatalogue.DATAEXTENSION)
      .map(file => new CommitLogFile(file.toString))
  }
}
