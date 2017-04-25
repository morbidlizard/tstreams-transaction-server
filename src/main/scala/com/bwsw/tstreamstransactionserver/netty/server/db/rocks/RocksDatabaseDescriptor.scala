package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import RocksDatabaseDescriptor._
import org.rocksdb._

case class RocksDatabaseDescriptor(name: Array[Byte], options: ColumnFamilyOptions, ttl: Integer = NoTTL)


object RocksDatabaseDescriptor {
  val NoTTL: Integer = int2Integer(-1)
}
