package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import com.bwsw.tstreamstransactionserver.netty.server.cache.Cacheable
import com.google.common.cache.CacheBuilder

class CacheLRU(size: Int)
  extends Cacheable[CacheKey, Array[Byte]]
{
  private val cache = CacheBuilder
    .newBuilder()
    .maximumSize(size)
    .build[CacheKey, Array[Byte]]()

  override def put(key: CacheKey, data: Array[Byte]): Boolean = {
    cache.put(key, data)
    true
  }

  override def get(key: CacheKey): Option[Array[Byte]] = {
    Option(cache.getIfPresent(key))
  }
}
