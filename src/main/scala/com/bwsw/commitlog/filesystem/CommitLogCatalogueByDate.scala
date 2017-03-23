package com.bwsw.commitlog.filesystem

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

object CommitLogCatalogueByDate {
  val MD5EXTENSION = ".md5"
  val DATAEXTENSION = ".dat"
}

/** Represents catalogue of specified date.
  *
  * @param rootPath path to the root directory of commitlog
  * @param date     date to link this object with
  */
class CommitLogCatalogueByDate(rootPath: String, date: Date) {
  //todo данный класс и CommitLogCatalogue связаны, как минимум, форматом создания папок.
  //todo Нужно либо объединить эти два класса, либо вынести формат в объект.
  //todo Позже обнаружила, что с FilePathManager имеется общая функциональность
  //todo + CommitLogFile существует в двух реализациях: private класс(внутри CommitLog) и обычный класс. Требуется рефакторинг
  import CommitLogCatalogueByDate._

  private val rootDirectory: String = rootPath


  private val dataFolder: File = {
    val simpleDateFormat = FilePathManager.simpleDateFormat
    val datePath = simpleDateFormat.format(date)
    val file = new File(rootDirectory, datePath)
    if (file.isDirectory) file else throw new IllegalArgumentException(s"Path ${file.getPath} is not directory!")
  }

  /**
    * For testing purposes only
    */
  def createFile(fileName: String): Boolean = {
    val commitLogFile = new File(dataFolder.toString, fileName + DATAEXTENSION)
    val md5File = new File(dataFolder.toString, fileName + MD5EXTENSION)

    commitLogFile.createNewFile() && md5File.createNewFile()
  }

  /** Removes specified file and its md5 file.
    *
    * @param fileName name of file to delete
    * @return true if file and its md5 file were deleted successfully
    */
  def deleteFile(fileName: String): Boolean = {
    val file = new File(dataFolder.toString, fileName)
    file.delete() &&
      new File(file.toString.dropRight(DATAEXTENSION.length) + MD5EXTENSION).delete()
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
      .filter(file => file.toString endsWith DATAEXTENSION)
      .map(file => new CommitLogFile(file.toString))
  }
}
