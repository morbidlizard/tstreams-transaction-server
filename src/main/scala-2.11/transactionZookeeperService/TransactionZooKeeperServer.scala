package transactionZookeeperService

import java.util.concurrent.Executors

import authService.AuthClient
import com.twitter.finagle.{ListeningServer, Thrift}
import com.twitter.util.{Await, Closable, Time, Future => TwitterFuture}
import org.apache.curator.retry.RetryNTimes
import transactionService.server.TransactionServer
import zooKeeper.ZKLeaderServer

class TransactionZooKeeperServer
  extends TransactionServer(configProperties.ServerConfig.transactionDataTtlAdd) with Closable {

  import configProperties.ServerConfig._

  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries),zkPrefix)

  zk.putData(transactionServerAddress.getBytes())

  private val server = Thrift.server
  val start: ListeningServer = server.serveIface(transactionServerAddress, this)
  override def close(deadline: Time): TwitterFuture[Unit] = start.close(deadline)

//  Runtime.getRuntime.addShutdownHook(new Thread() {
//    override def run(): Unit = {
//      streamService.StreamServiceImpl.entityStore.close()
//      streamService.StreamServiceImpl.environment.close()
//
//      transactionMetaService.TransactionMetaServiceImpl.entityStore.close()
//      transactionMetaService.TransactionMetaServiceImpl.environment.close()
//    }
//  })
}

object TransactionZooKeeperServer extends App {
  val server = new TransactionZooKeeperServer

  Await.ready(server.start)
}
