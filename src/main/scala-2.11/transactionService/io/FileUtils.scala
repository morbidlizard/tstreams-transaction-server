package transactionService.io

import java.io.File
import java.nio.file.{Files, Paths}
import resource.DB.PathToDatabases

object FileUtils {
  def createDirectory(name: String, path: String = PathToDatabases, deleteAtExit: Boolean = false): File = {
    val path = {
      val dir = Paths.get(s"/$PathToDatabases/$name")
      if (Files.exists(dir)) dir else java.nio.file.Files.createDirectories(Paths.get(s"/$PathToDatabases/$name"))
    }

    if (deleteAtExit)
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run() {
          org.apache.commons.io.FileUtils.forceDelete(path.toFile)
        }
      })
    path.toFile
  }

  def dirToFile(path: String) = new File(path)
}
