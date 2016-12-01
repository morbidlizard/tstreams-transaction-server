package transactionService.io

import java.io.File
import java.nio.file.{Files, Paths}
import configProperties.DB.PathToDatabases

object FileUtils {
  def createDirectory(name: String, path: String = PathToDatabases, deleteAtExit: Boolean = false): File = {
    val path = {
      val dir = Paths.get(s"/$PathToDatabases/$name")
      if (Files.exists(dir)) dir else java.nio.file.Files.createDirectories(Paths.get(s"/$PathToDatabases/$name"))
    }

    val file = path.toFile

    if (deleteAtExit) file.deleteOnExit()
    file
  }
}
