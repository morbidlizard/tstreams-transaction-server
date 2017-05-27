package com.bwsw.tstreamstransactionserver.netty.server.cache

trait Cacheable[Key, Data] {
  def put(key: Key, data: Data):Boolean
  def get(key: Key): Option[Data]
}
