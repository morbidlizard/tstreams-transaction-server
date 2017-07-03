package com.bwsw.tstreamstransactionserver.options

import java.util.Properties

class OptionHelper(properties: Properties) {
  def checkPropertyOnExistence(property: String)(classType: Class[_]): String = {
    Option(properties.getProperty(property))
      .getOrElse(throw new NoSuchElementException(
        s"No property by key: '$property' has been found for '${classType.getSimpleName}'." +
          s"You should define it and restart the program.")
      )
  }

  def castCheck[T](property: String,
                   constructor: String => T
                  )(implicit classType: Class[_]): T = {
    val value = checkPropertyOnExistence(property)(classType)
    try {
      constructor(value)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Property '$property' has got an invalid format, but expected another type."
        )
    }
  }
}
