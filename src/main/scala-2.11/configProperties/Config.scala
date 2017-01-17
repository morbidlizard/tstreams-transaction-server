package configProperties

import java.io.FileInputStream
import java.util.Properties

import exception.Throwables.ConfigNotFoundException

import scala.collection.Map

class Config(pathToConfig: String) {
  val properties: Properties = {
    val file = new Properties()
    scala.util.Try {
      val in = new FileInputStream(pathToConfig)
      file.load(in)
      in.close()
    } match {
      case scala.util.Success(_) => file
      case scala.util.Failure(_) => throw new ConfigNotFoundException
    }
  }

  import scala.collection.JavaConverters._
  def getAllProperties(prefix: String): Map[String, String] = properties.asScala.filterKeys(key => key.startsWith(prefix))

  def readPropertyOptional[T](property: String)(implicit funCast: String => T): Option[T] =
    Option(properties.getProperty(property)) match {
      case Some(property) => Some(funCast(property))
      case None => None
    }

  def readPropertyOptional[T](property: String, delimiter: Char)(implicit funCast: String => T): Option[Seq[T]] =
    Option(properties.getProperty(property)) match {
      case Some(property) => Some(property.split(delimiter) map funCast)
      case None => None
    }

  def readProperty[T](property: String)(implicit funCast: String => T): T =
    readPropertyOptional[T](property).getOrElse(throw new NoSuchElementException(s"$property isn't defined"))

  def readProperty[T](property: String, delimiter: Char)(implicit  funCast: String => T): Seq[T] =
    readPropertyOptional[T](property,delimiter).getOrElse(throw new NoSuchElementException(s"$property isn't defined"))

}

object Config {
  implicit def stringToBoolean: String => Boolean = str => augmentString(str).toBoolean
  implicit def stringToInt: String => Int = str => augmentString(str).toInt
  implicit def stringToShort: String => Short = str => augmentString(str).toShort
  implicit def stringToLong: String => Long = str => augmentString(str).toLong
  implicit def stringToFloat: String => Float = str => augmentString(str).toFloat
  implicit def stringToDouble: String => Double = str => augmentString(str).toDouble
}
