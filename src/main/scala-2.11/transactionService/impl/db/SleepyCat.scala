package transactionService.impl.db

import java.io.File
import java.lang.management.ManagementFactory
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.mongodb.operation.WriteOperation
import com.sleepycat.je._
import com.sleepycat.persist.{EntityCursor, EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, KeyField, Persistent, PrimaryKey}
import com.twitter.io.TempDirectory
import com.twitter.util.Time
import transactionService.impl.StreamServiceImpl

object SleepyCat extends App{



  @Entity
  class MyTransaction {
    @PrimaryKey var myPrimaryKey: MyKey = _
    var state: Int = _
    var timestamp: java.lang.Long = _
    var interval: java.lang.Long = _
    var quantity: Int = _

    def this(transactionID: java.lang.Long,
             state: Int,
             stream: Int,
             timestamp: java.lang.Long,
             interval: java.lang.Long,
             quantity: Int,
             partition: Int) {
      this()
      this.state = state
      this.timestamp = timestamp
      this.interval = interval
      this.quantity = quantity
      this.myPrimaryKey = new MyKey(stream, partition,transactionID)
    }

    override def toString: String = {s"$myPrimaryKey"}
  }


  @Persistent
  class MyKey {
    @KeyField(1) var stream: Int = _
    @KeyField(2) var partition: Int = _
    @KeyField(3) var transactionID: java.lang.Long = _
    def this(stream: Int, partition:Int, transactionID: java.lang.Long) = {
      this()
      this.stream = stream
      this.partition = partition
      this.transactionID = transactionID
    }

    override def toString: String = s"$stream $partition $transactionID"
  }

  class SimpleDA(store: EntityStore) {
    val pIdx = store.getPrimaryIndex(classOf[MyKey], classOf[MyTransaction])
  }

  def setup(directory: String =""): (Environment, EntityStore) = {
    val directory = new File("/home/revenskiy_ag/1")
    val myEnvConfig = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setLocking(true)

    val storeConfig = new StoreConfig()
      .setAllowCreate(true)
      .setTransactional(true)

    val myEnv = new Environment(directory, myEnvConfig)

    val store = new EntityStore(myEnv, "EntityStore", storeConfig)
    (myEnv,store)
  }

  def close: (Environment, EntityStore) => Unit = { (envr, store) =>
    store.close()
    envr.close()
  }


  def saveTransaction(transaction: MyTransaction, store: EntityStore) = {
    val simpleDA = new SimpleDA(store)
    simpleDA.pIdx.put(transaction)
  }


  def saveTransactions(transaction: Seq[MyTransaction], environment: Environment, store: EntityStore) = {
    val simpleDA = new SimpleDA(store)

    transaction foreach { transaction =>
      simpleDA.pIdx.putNoReturn(transaction)
    }
  }


  def saveTransactionsWithSetup(transaction: Seq[MyTransaction]) = {
    val (environment,store) = setup("1")
    val simpleDA = new SimpleDA(store)

    transaction foreach { transaction =>
      simpleDA.pIdx.put(transaction)
    }
    store.close()
    environment.close()
  }

  def saveAtomicallyTransaction(transaction: MyTransaction, environment: Environment, store: EntityStore) = {
    val simpleDA = new SimpleDA(store)

    val dbTransaction = environment.beginTransaction(null, null)

    simpleDA.pIdx.put(dbTransaction,transaction,Put.NO_OVERWRITE,new WriteOptions().setTTL(1,TimeUnit.HOURS))

    dbTransaction.commit()
  }


  def saveAtomicallyTransactions(transaction: Seq[MyTransaction], environment: Environment, store: EntityStore) = {
    val simpleDA = new SimpleDA(store)

    val dbTransaction = environment.beginTransaction(null, null)

    transaction foreach { transaction =>
      simpleDA.pIdx.putNoReturn(transaction)
    }

    dbTransaction.commit()
  }

  def saveAtomicallyTransactionsWithSetup(transaction: Seq[MyTransaction]) = {
    val (environment,store) = setup()
    val simpleDA = new SimpleDA(store)

    val dbTransaction = environment.beginTransaction(null, null)

    transaction foreach { transaction =>
      simpleDA.pIdx.putNoReturn(transaction)
    }

    dbTransaction.commit()

    store.close()
    environment.close()
  }



  def getTransactionRange(from: MyKey, to: MyKey, store: EntityStore): Array[MyTransaction] = {
    val simpleDA = new SimpleDA(store)

    import scala.collection.JavaConversions._
    val cursor = simpleDA.pIdx.entities(from, true, to, false)
    val transactions = cursor.iterator().toArray

    cursor.close()
    transactions
  }

  val (envHome,store) = setup()

//  val transactions = (1 to 10)
//    .foreach {_=>
//      val txn = new MyTransaction(java.time.Clock.systemUTC().millis(), 0, scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, scala.util.Random.nextInt(200))
//      saveAtomicallyTransaction(txn,envHome,store )
//    }


  val transactionsFromDb = getTransactionRange(new MyKey(Int.MinValue,Int.MinValue,0L),new MyKey(Int.MaxValue,Int.MaxValue,Long.MaxValue), store)

  transactionsFromDb foreach (x=> println(x))

  close(envHome,store)
}
