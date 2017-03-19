package com.bwsw.tstreamstransactionserver.netty

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.slf4j.LoggerFactory

object ObjectSerializer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def serialize(obj: Object): Seq[Byte] = {
    logger.debug(s"Serialize an object of class: '${obj.getClass}' to a byte array.")
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(obj)
    byteArrayOutputStream.toByteArray.toSeq
  }

  def deserialize(bytes: Seq[Byte]): Object = {
    logger.debug(s"Deserialize a byte array to an object.")
    val b = new ByteArrayInputStream(bytes.toArray)
    val o = new ObjectInputStream(b)
    o.readObject()
  }
}
