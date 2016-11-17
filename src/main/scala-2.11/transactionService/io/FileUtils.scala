package transactionService.io

import java.io.File
import java.nio.file.{Files, Paths}

object FileUtils {
  def createDirectory(name: String, deleteAtExit: Boolean = true): File = {
    val path = {
      val dir = Paths.get(name)
      if (Files.exists(dir)) dir else java.nio.file.Files.createDirectory(Paths.get(s"/tmp/$name"))
    }

    if (deleteAtExit)
      Runtime.getRuntime.addShutdownHook(new Thread {
        override def run() {
          org.apache.commons.io.FileUtils.forceDelete(path.toFile)
        }
      })
    path.toFile
  }
}
