package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import com.bwsw.tstreamstransactionserver.netty.server.cache.Cacheable
import com.google.common.cache.CacheBuilder

class CacheLRU(size: Int)
  extends Cacheable[KeyDataSeq, Array[Byte]]
{
  private val cache = CacheBuilder
    .newBuilder()
    .maximumSize(size)
    .build[KeyDataSeq, Array[Byte]]()

  override def put(key: KeyDataSeq, data: Array[Byte]): Boolean = {
    cache.put(key, data)
    true
  }

  override def get(key: KeyDataSeq): Option[Array[Byte]] = {
    Option(cache.getIfPresent(key))
  }
}
