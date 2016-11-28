package resource

import java.io.FileInputStream
import java.util.Properties

abstract class Config(pathToConfig: String) {
  private lazy val properties = {
    val file = new Properties()
    scala.util.Try {
      val in = new FileInputStream(pathToConfig)
      file.load(in)
      in.close()
    } match {
      case scala.util.Success(_) => file
      case scala.util.Failure(_) => throw new IllegalArgumentException("Config isn't found!")
    }
  }

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
  implicit def stringToSting: String => String = str => str
  def stringToInt: String => Int = str => str.toInt
  def stringToFloat: String => Float = str => str.toFloat
  def stringToLong: String => Long = str => str.toLong
}
