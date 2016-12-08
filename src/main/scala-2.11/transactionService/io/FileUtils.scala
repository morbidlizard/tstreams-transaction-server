package transactionService.io

import java.io.File
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.{Files, Paths}
import java.util

import configProperties.DB.PathToDatabases

object FileUtils {
  final private val perms = util.EnumSet.of(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE)

  def createDirectory(name: String, path: String = PathToDatabases, deleteAtExit: Boolean = false): File = {
    val path = {
      val dir = Paths.get(s"/$PathToDatabases/$name")
      if (Files.exists(dir)) dir
      else java.nio.file.Files.createDirectories(
        Paths.get(s"/$PathToDatabases/$name"),
        PosixFilePermissions.asFileAttribute(perms)
      )
    }

    val file = path.toFile

    if (deleteAtExit) file.deleteOnExit()
    file
  }
}
