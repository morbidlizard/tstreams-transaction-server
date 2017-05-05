package com.bwsw.tstreamstransactionserver.netty.server.streamService

import com.bwsw.tstreamstransactionserver.rpc.Stream

case class StreamRecord(key: StreamKey, stream: StreamValue) extends Stream {
  def id: Int = key.id
  override def name: String = stream.name
  override def partitions: Int = stream.partitions
  override def ttl: Long = stream.ttl
  override def description: Option[String] = stream.description
}
