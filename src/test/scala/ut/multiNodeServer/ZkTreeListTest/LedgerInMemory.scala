package ut.multiNodeServer.ZkTreeListTest

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.Ledger

class LedgerInMemory(id: Long)
  extends Ledger(id) {
  @volatile private var isClosed = false
  private val entryIDGen = new AtomicLong(0L)
  private val storage =
    new java.util.concurrent.ConcurrentHashMap[Long, Array[Byte]]()

  override def addEntry(data: Array[Byte]): Long = {
    if (isClosed)
      throw new IllegalAccessError()

    val id = entryIDGen.getAndIncrement()
    storage.put(id, data)
    id
  }

  override def readEntry(id: Long): Array[Byte] = {
    storage.get(id)
  }

  override def lastEntryID(): Long =
    entryIDGen.get() - 1L

  override def readRange(from: Long, to: Long): Array[Array[Byte]] = {
    val dataNumber = scala.math.abs(to - from + 1)
    val data = new Array[Array[Byte]](dataNumber.toInt)

    var index = 0
    var toReadIndex = from
    while (index < dataNumber) {
      data(index) = storage.get(toReadIndex)
      index = index + 1
      toReadIndex = toReadIndex + 1
    }
    data
  }

  override def close(): Unit = {
    isClosed = true
  }
}
