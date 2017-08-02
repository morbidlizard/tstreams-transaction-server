package com.bwsw.tstreamstransactionserver.netty.server.commitLogReader

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record

class BookKeeperRecordFrame(record: Record)
  extends Frame (
    record.recordType,
    record.timestamp,
    record.body
  )
