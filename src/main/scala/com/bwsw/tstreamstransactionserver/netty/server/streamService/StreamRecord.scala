package com.bwsw.tstreamstransactionserver.netty.server.streamService


import com.sun.xml.internal.messaging.saaj.util.ByteInputStream
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.netty.server.streamService
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, pretty, render}

case class StreamRecord(key: StreamKey, stream: streamService.StreamValue)
  extends rpc.Stream
{
  override def id: Int = key.id
  override def name: String = stream.name
  override def partitions: Int = stream.partitions
  override def ttl: Long = stream.ttl
  override def description: Option[String] = stream.description
  override def zkPath: String = stream.zkPath.get

  def toBinaryJson: Array[Byte] = {
    implicit val formats = DefaultFormats
    pretty(render(Extraction.decompose(this))).getBytes
  }
}

object StreamRecord {
  def fromBinaryJson(bytes: Array[Byte]): StreamRecord = {
    implicit val formats = DefaultFormats
    parse(new ByteInputStream(bytes, bytes.length))
      .extract[StreamRecord]
  }
}
