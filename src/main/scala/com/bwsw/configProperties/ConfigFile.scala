package com.bwsw.configProperties

import java.io.FileInputStream
import java.util.Properties

import com.bwsw.exception.Throwables.ConfigNotFoundException

class ConfigFile(pathToConfig: String) extends Config {
  import scala.collection.JavaConverters._
  override val properties: scala.collection.immutable.Map[String, String] = {
    val file = new Properties()
    scala.util.Try {
      val in = new FileInputStream(pathToConfig)
      file.load(in)
      in.close()
    } match {
      case scala.util.Success(_) => file.asScala.toMap
      case scala.util.Failure(_) => throw new ConfigNotFoundException
    }
  }
}
