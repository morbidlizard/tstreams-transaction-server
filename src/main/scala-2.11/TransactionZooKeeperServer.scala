import authService.ClientAuth
import com.twitter.finagle.Thrift
import transactionService.server.TransactionServiceImpl
import zooKeeper.{Agent, LeaderSelectorPriority, LeaderSelectorPriorityServer}
import com.twitter.util.Await


private object TransactionZooKeeperServer extends App {
  val addressZooKeeper = "172.17.0.2:2181"
  val path = "/stream_1"
  val agent= Agent("localhost", 8080, 1)
  val priority = LeaderSelectorPriority.Priority.Normal
  val clientAuth = new ClientAuth("localhost:8081")

  val server = Thrift.server
  val leaderSelectorByPriorityClient =
    new LeaderSelectorPriorityServer(addressZooKeeper,priority,path,agent.name)
  leaderSelectorByPriorityClient.start()


  //Await.result(server.start())
}
