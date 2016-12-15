package transactionService.server.transactionMetaService

import com.sleepycat.bind.tuple.TupleBinding
import com.sleepycat.je.{DatabaseEntry, SecondaryDatabase, SecondaryKeyCreator}
import transactionService.rpc.TransactionStates

object ProducerTransactionStateIndex extends SecondaryKeyCreator {
  override def createSecondaryKey(secondary: SecondaryDatabase, key: DatabaseEntry, data: DatabaseEntry, result: DatabaseEntry): Boolean = {
    val producerTransaction = ProducerTransaction.entryToObject(data)
    result.setData(java.nio.ByteBuffer.allocate(4).putInt(producerTransaction.state.value ^ 0x80000000).array())
    true
  }
}
