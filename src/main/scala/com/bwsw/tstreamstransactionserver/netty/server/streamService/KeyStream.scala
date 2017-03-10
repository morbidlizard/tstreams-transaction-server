package com.bwsw.tstreamstransactionserver.netty.server.streamService

case class KeyStream(val key: Key, stream: StreamWithoutKey) {
  def streamNameToLong = key.streamNameToLong
  def name = stream.name
  def partitions = stream.partitions
  def ttl  = stream.ttl
  def description = stream.description
}
