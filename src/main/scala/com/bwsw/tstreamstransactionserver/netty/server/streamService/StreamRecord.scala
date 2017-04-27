package com.bwsw.tstreamstransactionserver.netty.server.streamService

case class StreamRecord(key: StreamKey, stream: StreamValue) {
  def id: Long = key.id
  def name: String = stream.name
  def partitions: Int = stream.partitions
  def ttl: Long = stream.ttl
  def description: Option[String] = stream.description
}
