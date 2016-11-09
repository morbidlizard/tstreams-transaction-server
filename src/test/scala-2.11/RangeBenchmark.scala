import com.twitter.io.TempDirectory
import com.twitter.logging.{Level, Logger}
import com.twitter.util.Time
import org.scalameter.api._
import org.scalameter.picklers.Implicits._
import transactionService.impl.db.SleepyCat
import transactionService.impl.db.SleepyCat.{MyKey, MyTransaction}

object RangeBenchmark
  extends Bench[Double] {

  val interval = 1000
  val minValue = interval
  val maxValue = 10000

  val sizes = Gen.range("size")(minValue, maxValue, interval)

  lazy val executor = LocalExecutor(
    Executor.Warmer.Zero,
    Aggregator.min[Double],
    measurer)

  lazy val measurer = new Measurer.Default
  lazy val reporter = new MyReporter[Double]
  lazy val persistor = Persistor.None

  val ranges = for {
    size <- sizes
  } yield (size - interval, size)


  performance of "Range" in {
    val (env, store) = SleepyCat.setup()
    measure method "saveTransactions" in {
      using(ranges) curve("Range") config (
        exec.benchRuns -> 1
        ) in { r =>
        Logger.get().log(Level.INFO,s"${r._1} to ${r._2}")
        val transactions = (r._1 to r._2)
          .foreach{_=>
            val txn = new MyTransaction(scala.util.Random.nextLong(), 0, scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, 0)
            SleepyCat.saveAtomicallyTransaction(txn, env ,store)
          }
      }
    }
  }

//  performance of "Range" in {
//    measure method "saveTransactionsWithSetup" in {
//      using(ranges) config (
//        exec.benchRuns -> 1,
//        exec.maxWarmupRuns -> 1,
//        exec.minWarmupRuns -> 1
//        ) in { r =>
//        val transactions = (r._1 to r._2)
//          .map(_=> new MyTransaction(scala.util.Random.nextLong(), 0, scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, 0))
//        SleepyCat.saveTransactionsWithSetup(transactions)
//      }
//    }
//  }
//
//  measure method "saveTransactionalTransactions" in {
//    val (env, store) = SleepyCat.setup()
//    using(ranges) config (
//      exec.benchRuns -> 1,
//      exec.maxWarmupRuns -> 1
//      ) in { r =>
//      val transactions = (r._1 to r._2)
//        .map(_=> new MyTransaction(scala.util.Random.nextLong(), 0, scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, 0))
//      SleepyCat.saveTransactionalTransactions(transactions,env,store)
//    }
//  }


//  measure method "getTransactions" in {
//    val envHome = TempDirectory.create()
//    val transactions = (0 to 10000).map(_=> new MyTransaction(scala.util.Random.nextLong(), 0,
//      scala.util.Random.nextInt(), java.time.Clock.systemUTC().millis(), java.time.Clock.systemUTC().millis(), -1, 0))
//    using(ranges) config (
//      exec.benchRuns -> 5
//      ) in { r =>
//      SleepyCat.getTransactionRange(new MyKey(0,0,0L), new MyKey(999999999,9999999,999999999999999L), envHome)
//    }
//  }
}
