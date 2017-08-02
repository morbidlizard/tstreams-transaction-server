package com.bwsw.tstreamstransactionserver.netty.server.commitLogReader

import com.bwsw.commitlog.CommitLogRecord

class CommitLogRecordFrame(record: CommitLogRecord)
  extends Frame (
    record.messageType,
    record.timestamp,
    record.message
  )

