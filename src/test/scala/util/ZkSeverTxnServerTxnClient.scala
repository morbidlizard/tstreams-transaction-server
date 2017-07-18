package util

import com.bwsw.tstreamstransactionserver.netty.client.Client
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.SingleNodeServer
import org.apache.curator.test.TestingServer

class ZkSeverTxnServerTxnClient(val zkServer: TestingServer,
                                val transactionServer: SingleNodeServer,
                                val client: TTSClient
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
            client: TTSClient): ZkSeverTxnServerTxnClient =
    new ZkSeverTxnServerTxnClient(
      zkServer,
      transactionServer,
      client
    )
}

