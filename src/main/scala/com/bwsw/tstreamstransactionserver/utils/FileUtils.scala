package com.bwsw.tstreamstransactionserver.utils

import java.io.File
import java.nio.file.{Files, Path, Paths}

object FileUtils {
  def createDirectory(name: String, path: String): File = {
    val pathNew = {
      val dir: Path = Paths.get(s"/$path/$name")
      if (Files.exists(dir)) dir
      else java.nio.file.Files.createDirectories(Paths.get(s"/$path/$name"))
    }.toFile
    pathNew
  }
}
