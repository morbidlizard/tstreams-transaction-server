package ut.multiNodeServer.ZkTreeListTest

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{Ledger, StorageManager}

class StorageManagerInMemory
  extends StorageManager{

  private val ledgerIDGen = new AtomicLong(0L)

  private val storage =
    new java.util.concurrent.ConcurrentHashMap[Long, Ledger]()

  override def addLedger(): Ledger = {
    val id = ledgerIDGen.getAndIncrement()

    val previousLedger = storage.computeIfAbsent(id, {id =>
      new LedgerInMemory(id)
    })

    if (previousLedger != null)
      previousLedger
    else
      throw new IllegalArgumentException()
  }

  override def getLedger(id: Long): Option[Ledger] = {
    Option(storage.get(id))
  }

  override def deleteLedger(id: Long): Boolean = {
    Option(storage.remove(id)).isDefined
  }
}
