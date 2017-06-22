package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

class TimestampRecord(override val timestamp: Long)
  extends Record(RecordType.Timestamp, timestamp, Array.emptyByteArray)
