package com.bwsw.tstreamstransactionserver.netty.server.db.berkeley

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.sleepycat.je.{Database, DatabaseEntry, Transaction}

final class BerkeleyDbBatch(batch: Transaction,
                            databaseHandlers: collection.immutable.Map[Int, Database])
  extends KeyValueDbBatch {

  override def put(index: Int,
                   key: Array[Byte],
                   data: Array[Byte]): Boolean = {
    scala.util.Try(
      databaseHandlers(index)
        .put(
          batch,
          new DatabaseEntry(key),
          new DatabaseEntry(data)
        )
    ).isSuccess
  }

  override def remove(index: Int,
                      key: Array[Byte]): Unit = {
    databaseHandlers(index)
      .delete(
        batch,
        new DatabaseEntry(key)
      )
  }

  override def write(): Boolean = {
    scala.util.Try(
      batch.commit()
    ).isSuccess
  }
}
