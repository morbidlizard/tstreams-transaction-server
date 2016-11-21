import authService.ClientAuth
import com.twitter.finagle.{ListeningServer, Thrift}
import transactionService.server.TransactionServiceImpl
import zooKeeper.{Agent, LeaderSelectorByPriorityClient}
import com.twitter.util.{Await, Future => TwitterFuture}

class TransactionZooKeeperServer(addressZooKeeper: String,
                                 path: String,
                                 agent: Agent,
                                 priority : LeaderSelectorByPriorityClient.Priority.Value,
                                 clientAuth: ClientAuth)
  extends TransactionServiceImpl(clientAuth)
{
//  private lazy val server = Thrift.server
//  private lazy val leaderSelectorByPriorityClient =
//    new LeaderSelectorByPriorityClient(addressZooKeeper,path,agent.name,priority)
//
//  def start(): TwitterFuture[Unit] = TwitterFuture {
//    leaderSelectorByPriorityClient.start()
//  } foreach (_ => server)
//
//  def close(): TwitterFuture[Unit] = TwitterFuture {
//    leaderSelectorByPriorityClient.close()
//    server.close()
//  }
//
//  def getMasterAddress = leaderSelectorByPriorityClient.getLeader
}

object TransactionZooKeeperServer extends App {
  val addressZooKeeper = "172.17.0.2:2181"
  val path = "/stream_1"
  val agent= Agent("localhost", 8080, 1)
  val priority = LeaderSelectorByPriorityClient.Priority.Normal
  val clientAuth = new ClientAuth("localhost:8081")

  private lazy val server = Thrift.server
  private lazy val leaderSelectorByPriorityClient =
    new LeaderSelectorByPriorityClient(addressZooKeeper,path,agent.name,priority)
  leaderSelectorByPriorityClient.start()


  Await.result(server.serveIface(agent.name, new TransactionServiceImpl(clientAuth)))

  //Await.result(server.start())
}
