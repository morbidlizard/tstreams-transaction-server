package com.bwsw.tstreamstransactionserver.netty.server.db.berkeley

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDb, KeyValueDbIterator}
import com.bwsw.tstreamstransactionserver.netty.server.db.berkeley.BerkeleyDb._
import com.sleepycat.je._

private object BerkeleyDb {
  val lockMode: LockMode = LockMode.READ_UNCOMMITTED_ALL
}


final class BerkeleyDb(database: Database)
  extends KeyValueDb {

  override def get(key: Array[Byte]): Array[Byte] = {
    val keyEntry =
      new DatabaseEntry(key)
    val valueEntry =
      new DatabaseEntry()

    database.get(
      null,
      keyEntry,
      valueEntry,
      lockMode
    )
    valueEntry.getData
  }

  override def put(key: Array[Byte],
                   data: Array[Byte]): Boolean = {
    val keyEntry =
      new DatabaseEntry(key)
    val valueEntry =
      new DatabaseEntry(data)

    database.put(
      null,
      keyEntry,
      valueEntry
    ) == OperationStatus.SUCCESS
  }

  override def delete(key: Array[Byte]): Boolean = {
    val keyEntry =
      new DatabaseEntry(key)

    database.delete(
      null,
      keyEntry
    ) == OperationStatus.SUCCESS
  }

  override def iterator: KeyValueDbIterator = {
    new BerkeleyDbIterator(
      database.openCursor(
        null,
        CursorConfig.READ_UNCOMMITTED
      )
    )
  }
}
