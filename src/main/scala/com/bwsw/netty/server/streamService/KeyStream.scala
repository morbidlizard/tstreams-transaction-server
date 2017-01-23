package com.bwsw.netty.server.streamService

case class KeyStream(key: Key, stream: Stream) {
  def streamNameToLong = key.streamNameToLong
  def name = stream.name
  def partitions = stream.partitions
  def ttl  = stream.ttl
  def description = stream.description
}
