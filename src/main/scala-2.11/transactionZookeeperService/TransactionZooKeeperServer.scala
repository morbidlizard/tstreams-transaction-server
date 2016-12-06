package transactionZookeeperService

import authService.AuthClient
import com.sleepycat.je.CursorConfig
import com.twitter.finagle.{ListeningServer, Thrift}
import com.twitter.logging.Level
import com.twitter.util.{Await, Closable, Time, Future => TwitterFuture}
import org.apache.curator.retry.RetryNTimes
import transactionService.rpc.TransactionStates
import transactionService.server.{TransactionServer, streamService, transactionMetaService}
import transactionService.server.transactionMetaService.ProducerTransaction
import zooKeeper.ZKLeaderServer

class TransactionZooKeeperServer
  extends TransactionServer({
    import configProperties.ServerConfig._
    new AuthClient(authAddress, authTimeoutConnection, authTimeoutBetweenRetries)
  }, configProperties.ServerConfig.transactionDataTtlAdd) with Closable {

  import configProperties.ServerConfig._


  val zk = new ZKLeaderServer(zkEndpoints,zkTimeoutSession,zkTimeoutConnection,
    new RetryNTimes(zkRetriesMax, zkTimeoutBetweenRetries),zkPrefix)

  zk.putData(transactionServerAddress.getBytes())

  private val server = Thrift.server
  val start: ListeningServer = server.serveIface(transactionServerAddress, this)
  override def close(deadline: Time): TwitterFuture[Unit] = start.close(deadline)


  private def transiteTxnsToInvalidState() = {
    import transactionService.server.transactionMetaService.TransactionMetaServiceImpl._
    val transactionDB = environment.beginTransaction(null, null)

    val entities = producerSecondaryIndex.subIndex(TransactionStates.Opened.getValue()).entities(transactionDB, new CursorConfig())

    var txn = entities.next()
    while (txn != null) {
      logger.log(Level.INFO, s"${txn.toString} transit it's state to Invalid!")
      val newInvalidTxn = new ProducerTransaction(txn.transactionID, TransactionStates.Invalid, txn.stream, txn.timestamp, txn.quantity, txn.partition)
      producerPrimaryIndex.put(transactionDB, newInvalidTxn)
      txn = entities.next()
    }

    entities.close()
    transactionDB.commit()
  }

  transiteTxnsToInvalidState()

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      streamService.StreamServiceImpl.entityStore.close()
      streamService.StreamServiceImpl.environment.close()

      transactionMetaService.TransactionMetaServiceImpl.entityStore.close()
      transactionMetaService.TransactionMetaServiceImpl.environment.close()
    }
  })
}

object TransactionZooKeeperServer extends App {
  val server = new TransactionZooKeeperServer

  Await.ready(server.start)
}
