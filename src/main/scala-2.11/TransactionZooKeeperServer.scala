import authService.ClientAuth
import com.twitter.finagle.Thrift
import transactionService.server.TransactionServer
import com.twitter.util.Await
import org.apache.curator.retry.ExponentialBackoffRetry
import resource.ConfigServer
import zooKeeper.ZKLeaderServer


class TransactionZooKeeperServer(val clientAuth: ClientAuth, config: ConfigServer)
  extends TransactionServer(clientAuth) {
  import config._

  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new ExponentialBackoffRetry(zkTimeoutBetweenRetries,zkRetriesMax),zkPrefix)

  zk.putData(transactionServerAddress.getBytes())

  private val server = Thrift.server
  def serve = server.serveIface(transactionServerAddress, this)


}

object TransactionZooKeeperServer extends App {
  val config = new ConfigServer("serverProperties.properties")
  import config._
  val server = new TransactionZooKeeperServer(new ClientAuth(authAddress,authTimeoutConnection,authTimeoutExponentialBetweenRetries),config)

  Await.ready(server.serve)
}
