package util

import com.bwsw.tstreamstransactionserver.netty.server.Server
import org.apache.curator.test.TestingServer

final class ZkSeverAndTransactionServer(val zkServer: TestingServer,
                                        val transactionServer: Server
                                 )
{
  def close(): Unit = {
    transactionServer.shutdown()
    zkServer.close()
  }
}

object ZkSeverAndTransactionServer{
  def apply(zkServer: TestingServer, transactionServer: Server): ZkSeverAndTransactionServer =
    new ZkSeverAndTransactionServer(zkServer, transactionServer)
}
