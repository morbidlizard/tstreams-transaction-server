package com.bwsw.commitlog.filesystem

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{DirectoryFileFilter, NotFileFilter, TrueFileFilter}

class CommitLogCatalogueAllDates(rootPath: String) {
  private val format = {
    val format = new java.text.SimpleDateFormat("yyyy/MM/dd")
    format.setLenient(false)
    format
  }

  private val dataFolders = {
    import scala.collection.JavaConverters._
    val rootDirectorySizeWithSlash = rootPath.length + 1
    val allFolders = FileUtils.listFilesAndDirs(new File(rootPath), new NotFileFilter(TrueFileFilter.TRUE), DirectoryFileFilter.DIRECTORY)
    allFolders.asScala
      .map(path => path.getPath.drop(rootDirectorySizeWithSlash))
      .filter {path =>
        scala.util.Try(format.parseObject(path)).isSuccess
      }.toSeq
  }
  def catalogues = dataFolders.map(dataFolder => new CommitLogCatalogue(rootPath, format.parse(dataFolder)))
}

