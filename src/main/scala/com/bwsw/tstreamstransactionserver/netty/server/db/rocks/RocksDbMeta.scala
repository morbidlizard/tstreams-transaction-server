package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

case class RocksDbMeta(name: String) {
  val binaryName: Array[Byte] = name.getBytes()
  val id: Int = java.util.Arrays.hashCode(binaryName)
}
