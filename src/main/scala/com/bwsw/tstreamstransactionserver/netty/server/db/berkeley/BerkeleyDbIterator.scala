package com.bwsw.tstreamstransactionserver.netty.server.db.berkeley

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbIterator
import com.bwsw.tstreamstransactionserver.netty.server.db.berkeley.BerkeleyDbIterator._
import com.sleepycat.je._

private object BerkeleyDbIterator {
  val lockMode: LockMode =
    LockMode.READ_UNCOMMITTED_ALL
}

final class BerkeleyDbIterator(cursor: Cursor)
  extends KeyValueDbIterator {

  private val currentKey =
    new DatabaseEntry()
  private val currentData =
    new DatabaseEntry()

  private var isValidLastOperation: Boolean =
    false

  override def key(): Array[Byte] = {
    currentKey.getData
  }

  override def value(): Array[Byte] = {
    currentData.getData
  }

  override def isValid: Boolean = {
    isValidLastOperation
  }

  override def seekToFirst(): Unit = {
    isValidLastOperation = cursor.getFirst(
      currentKey,
      currentData,
      lockMode
    ) == OperationStatus.SUCCESS
  }

  override def seekToLast(): Unit = {
    isValidLastOperation = cursor.getLast(
      currentKey,
      currentData,
      lockMode
    ) == OperationStatus.SUCCESS
  }

  override def seek(target: Array[Byte]): Unit = {
    currentKey.setData(target)
    isValidLastOperation = cursor.getSearchKeyRange(
      currentKey,
      currentData,
      lockMode
    ) == OperationStatus.SUCCESS
  }

  override def next(): Unit = {
    isValidLastOperation = cursor.getNext(
      currentKey,
      currentData,
      lockMode
    ) == OperationStatus.SUCCESS
  }

  override def prev(): Unit = {
    isValidLastOperation = cursor.getPrev(
      currentKey,
      currentData,
      lockMode
    ) == OperationStatus.SUCCESS
  }

  override def close(): Unit = {
    isValidLastOperation = false
    cursor.close()
  }
}
