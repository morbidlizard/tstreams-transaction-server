package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

import com.bwsw.tstreamstransactionserver.netty.server.RecordType

class TimestampRecord(override val timestamp: Long)
  extends Record(RecordType.Timestamp, timestamp, Array.emptyByteArray)
