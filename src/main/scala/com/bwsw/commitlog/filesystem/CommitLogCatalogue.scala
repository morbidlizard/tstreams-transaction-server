package com.bwsw.commitlog.filesystem

import java.io.File
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, NotFileFilter, TrueFileFilter}

trait ICommitLogCatalogue {
  def catalogues: Seq[CommitLogCatalogueByDate]
}

class CommitLogCatalogue(rootPath: String) extends ICommitLogCatalogue {
  private val formatter = {
    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
    format.setLenient(false)
    format
  }

  private def dataFolders = {
    import scala.collection.JavaConverters._
    val rootDirectorySizeWithSlash = rootPath.length + 1
    val allFolders = FileUtils.listFilesAndDirs(new File(rootPath), new NotFileFilter(TrueFileFilter.TRUE), DirectoryFileFilter.DIRECTORY)
    allFolders.asScala
      .map(path => path.getPath.drop(rootDirectorySizeWithSlash))
      .filter { path =>
        scala.util.Try(formatter.parseObject(path)).isSuccess
      }.toSeq
  }

  def catalogues = {
    dataFolders.map(dataFolder => new CommitLogCatalogueByDate(rootPath, formatter.parse(dataFolder)))
  }

  /**
    * For testing purposes only
    */
  def createCatalogue(date: Date) = {
    val directoryPath = formatter.format(date)
    new File(rootPath + "/" + directoryPath).mkdirs()
  }
}