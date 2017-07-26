package com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService

import com.bwsw.tstreamstransactionserver.netty.server.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.CommitLogKey

class CommitLogService(rocksDB: KeyValueDbManager)
{
  private val commitLogDatabase = rocksDB.getDatabase(RocksStorage.COMMIT_LOG_STORE)
  private[server] final def getLastProcessedCommitLogFileID: Option[Long] = {
    val iterator = commitLogDatabase.iterator
    iterator.seek(BigCommit.commitLogKey)

    val id = if (iterator.isValid)
      Some(CommitLogKey.fromByteArray(iterator.value()).id)
    else
      None

    iterator.close()
    id
  }
}
