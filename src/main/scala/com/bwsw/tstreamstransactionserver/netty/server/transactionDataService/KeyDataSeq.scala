package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService


case class KeyDataSeq(key: Key, dataID: Int){
  final def toByteArray: Array[Byte] = key.toByteArray(dataID)
}

