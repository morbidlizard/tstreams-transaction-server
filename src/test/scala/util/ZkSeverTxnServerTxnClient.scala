package util

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.server.SingleNodeServer
import org.apache.curator.test.TestingServer

class ZkSeverTxnServerTxnClient(val zkServer: TestingServer,
                                val transactionServer: SingleNodeServer,
                                val client: Client
                               )
{
  def close(): Unit = {
    transactionServer.shutdown()
    zkServer.close()
    client.shutdown()
  }
}

object ZkSeverTxnServerTxnClient {
  def apply(zkServer: TestingServer,
            transactionServer: SingleNodeServer,
            client: Client): ZkSeverTxnServerTxnClient =
    new ZkSeverTxnServerTxnClient(
      zkServer,
      transactionServer,
      client
    )
}

