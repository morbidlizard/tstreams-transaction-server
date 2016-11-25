package transactionService.server

import com.twitter.finagle.Thrift
import com.twitter.util.{Await, Future, FuturePool, FuturePools}
import org.apache.curator.retry.ExponentialBackoffRetry
import transactionService.server.transactionDataService.TransactionDataServiceImpl
import transactionService.server.streamService.StreamServiceImpl
import transactionService.server.сonsumerService.ConsumerServiceImpl
import transactionService.rpc.TransactionService
import transactionService.server.transactionMetaService.TransactionMetaServiceImpl
import resource.ZooKeeper._
import zooKeeper.ZKLeaderServer

class TransactionServiceImpl(val address: String,
                             val zkAddress: String,
                             val zkSessionTimeout: Int,
                             val zkConnectionTimeout: Int,
                             val zkTimeoutBetweenRetries: Int,
                             val zkMaxRetriesNumber: Int,
                             val zkPrefix: String,
                             override val authClient: authService.ClientAuth)
  extends TransactionService[Future]
    with ConsumerServiceImpl
    with StreamServiceImpl
    with TransactionMetaServiceImpl
    with TransactionDataServiceImpl
{
  val zKLeaderServer = new ZKLeaderServer(zkAddress, zkSessionTimeout, zkConnectionTimeout, new ExponentialBackoffRetry(zkTimeoutBetweenRetries, zkMaxRetriesNumber), zkPrefix)
  def putMasterToZooKeeper = zKLeaderServer.putData(address.getBytes())
}

object TransactionServiceImpl extends App{

  val address = "localhost:8080"
  val server = Thrift.server
  val service = new TransactionServiceImpl(address,Address,SessionTimeout,ConnectionTimeout,TimeoutBetweenRetries,MaxRetriesNumber,Prefix, new authService.ClientAuth(":8081",50000,5000))
  service.putMasterToZooKeeper

  Await.ready(server.serveIface(address, service))
  //#thriftserverapi
}
