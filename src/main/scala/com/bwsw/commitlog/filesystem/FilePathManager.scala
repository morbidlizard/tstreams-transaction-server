package com.bwsw.commitlog.filesystem

import java.io.{File, FilenameFilter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.GregorianCalendar

/**
  * Created by Ivan Kudryavtsev on 26.01.2017.
  */

object FilePathManager {
  val DATAEXTENSION = ".dat"
  val MD5EXTENSION = ".md5"
}

/** Manages commitlog filesystem.
  *
  * @param rootDir root directory of commitlog
  */
class FilePathManager(rootDir: String) {
  private val rootPath: File = new File(rootDir)
  rootPath.mkdirs()
  private var nextID: Int = -1
  private val datFilter = new FilenameFilter() {
    override def accept(dir: File, name: String): Boolean = {
      name.toLowerCase().endsWith(FilePathManager.DATAEXTENSION)
    }
  }

  if(!rootPath.isDirectory())
    throw new IllegalArgumentException(s"Path $rootDir doesn't exists.")

  def getCurrentPath: String = Paths.get(rootDir, /*curDate,*/ nextID.toString).toString

  def getNextPath: String = {
    if (nextID >= 0) {
      nextID += 1
      val nextPath = Paths.get(rootDir, nextID.toString).toString
      return nextPath
    }

    if (createPath()) {
      nextID = 0
      Paths.get(rootDir, nextID.toString).toString
    } else {
      val filesDir = new File(rootDir)
      var max = -1

      filesDir.listFiles(datFilter).foreach(f => {
        if (Integer.parseInt(f.getName.split("\\.")(0)) > max) max = Integer.parseInt(f.getName.split("\\.")(0))
      })
      nextID = max + 1

      Paths.get(rootDir, nextID.toString).toString
    }
  }

  private def createPath(): Boolean = {
    val path = new File(rootDir)
    if(!(path.exists() && path.isDirectory())) {
      path.mkdirs()
      true
    } else false
  }
}
