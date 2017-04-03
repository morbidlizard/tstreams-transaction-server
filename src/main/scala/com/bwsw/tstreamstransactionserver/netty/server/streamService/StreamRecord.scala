package com.bwsw.tstreamstransactionserver.netty.server.streamService

case class StreamRecord(key: StreamKey, stream: StreamValue) {
  def streamNameToLong = key.streamNameAsLong
  def name = stream.name
  def partitions = stream.partitions
  def ttl  = stream.ttl
  def description = stream.description
}
