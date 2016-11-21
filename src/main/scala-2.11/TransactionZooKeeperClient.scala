//import java.nio.ByteBuffer
//import java.util.concurrent.atomic.LongAdder
//
//import authService.ClientAuth
//import com.twitter.util.{Future => TwitterFuture}
//import transactionService.rpc.Stream
//import transactionService.client.ClientTransaction
//import transactionService.rpc.{Transaction, TransactionService}
//import zooKeeper.{Agent, ZooTree}
//
//class TransactionZooKeeperClient(login: String, password: String, val zooKeeperClient: ZooTree, clientAuth: ClientAuth) {
//
////  private val index = new LongAdder()
////  val transactionClients = {
////    val map = new java.util.concurrent.ConcurrentHashMap[Int, ClientTransaction]()
////    agents.foreach { agent =>
////      val client = s"${agent.address}:${agent.port}"
////      map.putIfAbsent({index.increment(); index.intValue()}, new ClientTransaction(login, password, client, clientAuth))
////    }
////    map
////  }
//
//  // coordination.hosts = 192.1.1.1:2181,192.1.1.2:2181,192.1.1.3:2181
//  // coordination.prefix = /ab/c/d/ -> master {"1.2.3.4", 8888} - ephemeral node
//  // coordination.session.timeout_ms = 5000
//
//  // listen.address = "1.2.3.4"
//  // listen.port = 8888
//
//  //Stream API
//  override def putStream(token: String, stream: String, partitions: Int, description: Option[String]): TwitterFuture[Boolean] = {
//    zooKeeperClient.addStream(stream,partitions)
//    transactionServiceClient.putStream(token, stream, partitions, description)
//  }
//  override def isStreamExist(token: String, stream: String): TwitterFuture[Boolean] = transactionServiceClient.isStreamExist(token, stream)
//  override def getStream(token: String, stream: String): TwitterFuture[Stream]  = transactionServiceClient.getStream(token, stream)
//  override def delStream(token: String, stream: String): TwitterFuture[Boolean] = transactionServiceClient.delStream(token, stream)
//
//  //TransactionMeta API
//  override def putTransaction(token: String, transaction: Transaction): TwitterFuture[Boolean] = {
//    transactionServiceClient.putTransaction(token, transaction)
//  }
//  override def putTransactions(token: String, transactions: Seq[Transaction]): TwitterFuture[Boolean] = {
//    transactionServiceClient.putTransactions(token, transactions)
//  }
//  override def scanTransactions(token: String, stream: String, partition: Int): TwitterFuture[Seq[Transaction]] = transactionServiceClient.scanTransactions(token, stream, partition)
//  override def scanTransactionsCRC32(token: String, stream: String, partition: Int): TwitterFuture[Int] = transactionServiceClient.scanTransactionsCRC32(token, stream, partition)
//
//  //TransactionData API
//  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): TwitterFuture[Boolean] =
//  transactionServiceClient.putTransactionData(token, stream, partition, transaction, from, data)
//
//  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
//    transactionServiceClient.getTransactionData(token,stream,partition,transaction,from,to)
//
//  //Consumer API
//  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): TwitterFuture[Boolean] =
//  transactionServiceClient.setConsumerState(token,name,stream,partition,transaction)
//  override def getConsumerState(token: String, name: String, stream: String, partition: Int): TwitterFuture[Long] =
//    transactionServiceClient.getConsumerState(token,name,stream,partition)
//}
