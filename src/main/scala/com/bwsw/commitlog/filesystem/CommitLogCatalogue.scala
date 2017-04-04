//package com.bwsw.commitlog.filesystem
//
//import java.io.File
//import java.util.Date
//
//import org.apache.commons.io.FileUtils
//import org.apache.commons.io.filefilter.{DirectoryFileFilter, NotFileFilter, TrueFileFilter}
//
//trait ICommitLogCatalogue {
//  def catalogues: Seq[CommitLogCatalogueByDate]
//}
//
//class CommitLogCatalogue(rootPath: String) extends ICommitLogCatalogue {
//  private def dataFolders = {
//    import scala.collection.JavaConverters._
//    val rootDirectorySizeWithSlash = rootPath.length + 1
//    val allFolders = FileUtils.listFilesAndDirs(new File(rootPath), new NotFileFilter(TrueFileFilter.TRUE), DirectoryFileFilter.DIRECTORY)
//    allFolders.asScala
//      .map(path => path.getPath.drop(rootDirectorySizeWithSlash))
//      .filter { path =>
//        scala.util.Try(FilePathManager.simpleDateFormat.parseObject(path)).isSuccess
//      }.toSeq
//  }
//
//  def catalogues = {
//    dataFolders.map(dataFolder => new CommitLogCatalogueByDate(rootPath, FilePathManager.simpleDateFormat.parse(dataFolder)))
//  }
//
//  /**
//    * For testing purposes only
//    */
//  def createCatalogue(date: Date) = {
//    val directoryPath = FilePathManager.simpleDateFormat.format(date)
//    new File(rootPath + File.separator + directoryPath).mkdirs()
//  }
//}