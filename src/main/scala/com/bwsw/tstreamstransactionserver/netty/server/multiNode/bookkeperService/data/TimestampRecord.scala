package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data

import com.bwsw.tstreamstransactionserver.netty.server.commitLogReader.Frame

class TimestampRecord(override val timestamp: Long)
  extends Record(Frame.Timestamp.id.toByte, timestamp, Array.emptyByteArray) {
}
